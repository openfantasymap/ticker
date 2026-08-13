// A generic tick service.
//
//   node services/ticker/ticker.js
//
// Games that share a world need to share a clock, and a browser is a poor
// clock: it throttles when backgrounded, it stops when closed, and electing one
// player's tab to keep time for everyone makes the world only as reliable as
// whoever happened to join first. So time comes from here instead — one process
// that knows nothing about any particular game.
//
// CONFIGURATION IS SPLIT, and the split is the whole design:
//
//   ENVIRONMENT says where to publish. One broker per deployment.
//   LABELS say what a tick IS, what it MEANS, and how it STARTS. Those are
//   properties of the game that needs ticking, so they travel with that game's
//   container and change without this service being touched or redeployed.
//
// A "family" is a class of tick — `cities-world`, `combat-round`, `season` —
// and a "key" is one instance of it, usually the world or table. Several games
// can want ticks at once, at different rates, meaning different amounts of
// world time, and none of them need to know about each other.
//
// TIME IS DERIVED, NOT COUNTED. A family with an epoch publishes
//
//     tick = origin + floor((now - epoch) / interval)
//
// which means this process holds no authority it could lose. Restart it, run
// two of them, or lose the broker for an hour: the tick afterwards is the tick
// that would have been, and any client can compute the same number without
// asking. The published message is a convenience and a heartbeat, not a source
// of truth. Only `start=now` families need persistence, because their epoch is
// the moment they were first seen and nothing else records it.

import fs from 'node:fs';
import path from 'node:path';
import { MqttClient } from './mqtt-min.js';
import * as docker from './docker.js';

/* ------------------------------------------------------ environment ------ */
const env = process.env;
const BROKER = env.MQTT_URL ?? env.TICKER_MQTT_URL ?? 'mqtt://localhost:1883';
const TOPIC_ROOT = env.TICK_TOPIC_ROOT ?? 'ofm/tick';
const STATE_FILE = env.TICK_STATE ?? '/var/lib/ofm-ticker/state.json';
const POLL_MS = Number(env.TICK_DISCOVER_MS ?? 30000);
const ID = env.TICKER_ID ?? `ticker-${process.pid}`;
const DRY = env.TICK_DRY_RUN === 'true';
const LOG = (env.TICK_LOG ?? 'info').toLowerCase();

const log = (level, ...a) => {
  const order = { error: 0, warn: 1, info: 2, debug: 3 };
  if ((order[level] ?? 2) <= (order[LOG] ?? 2)) {
    console.log(`${new Date().toISOString()} ${level.padEnd(5)}`, ...a);
  }
};

/* ------------------------------------------------------ label parsing ---- */

/** "500ms", "30s", "5m", "2h", "1d", or a bare number of seconds. */
export function duration(text, fallback) {
  if (text == null || text === '') return fallback;
  const m = String(text).trim().match(/^(-?[\d.]+)\s*(ms|s|m|h|d|w)?$/i);
  if (!m) return fallback;
  const n = Number(m[1]);
  if (!Number.isFinite(n)) return fallback;
  const unit = (m[2] ?? 's').toLowerCase();
  const mul = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }[unit];
  return n * mul;
}

/**
 * How much WORLD time one tick represents.
 *
 * Accepts a plain duration ("6h", "1d") and the calendar units a duration
 * cannot express, because months and years are not fixed lengths and a game
 * that ticks monthly means calendar months, not 30-day blocks. Those are kept
 * as a unit + count so the calendar maths stays honest instead of being
 * flattened into an average number of milliseconds.
 */
export function represents(text, fallback = { unit: 'month', count: 1 }) {
  if (!text) return fallback;
  const s = String(text).trim().toLowerCase();
  const m = s.match(/^(\d+(?:\.\d+)?)?\s*(ms|millisecond|s|sec|second|m|min|minute|h|hour|d|day|w|week|mo|month|y|yr|year)s?$/);
  if (!m) return fallback;
  const count = m[1] ? Number(m[1]) : 1;
  const u = m[2];
  const unit = /^(mo|month)/.test(u) ? 'month'
    : /^(y|yr|year)/.test(u) ? 'year'
      : /^(w|week)/.test(u) ? 'week'
        : /^(d|day)/.test(u) ? 'day'
          : /^(h|hour)/.test(u) ? 'hour'
            : /^(m|min|minute)/.test(u) ? 'minute'
              : /^(s|sec|second)/.test(u) ? 'second' : 'millisecond';
  return { unit, count };
}

/** Advance a calendar date by `n` of a world-time unit. Months stay months. */
export function advance(date, { unit, count }, n) {
  const d = new Date(date.getTime());
  const k = count * n;
  switch (unit) {
    case 'year': d.setUTCFullYear(d.getUTCFullYear() + k); break;
    case 'month': d.setUTCMonth(d.getUTCMonth() + k); break;
    case 'week': d.setUTCDate(d.getUTCDate() + 7 * k); break;
    case 'day': d.setUTCDate(d.getUTCDate() + k); break;
    case 'hour': d.setUTCHours(d.getUTCHours() + k); break;
    case 'minute': d.setUTCMinutes(d.getUTCMinutes() + k); break;
    case 'second': d.setUTCSeconds(d.getUTCSeconds() + k); break;
    default: d.setUTCMilliseconds(d.getUTCMilliseconds() + k);
  }
  return d;
}

/**
 * One family's rules, from its labels.
 *
 * Every field is optional and every default is stated, so a container can opt
 * in with two labels and still get something sensible.
 */
export function planOf(entry, saved) {
  const l = entry.labels;
  const intervalMs = Math.max(50, duration(l.interval, 60000));
  const speed = Math.max(0, Number(l.speed ?? 1) || 1);
  const origin = Math.trunc(Number(l.origin ?? 0) || 0);
  const mode = (l.start ?? 'epoch').toLowerCase();

  // The epoch is the instant at which the tick counter equals `origin`.
  //
  //   epoch  an explicit instant. Reproducible, survives everything, and two
  //          tickers started a week apart agree to the millisecond.
  //   now    the moment this family was FIRST seen, persisted thereafter.
  //          Convenient for a scratch game; the one mode that needs state.
  //   paused holds at `origin` and publishes it, so clients still learn the
  //          rules and the world simply does not move.
  let epoch = null;
  if (mode === 'paused') epoch = null;
  else if (l.epoch) {
    const parsed = Date.parse(l.epoch);
    epoch = Number.isFinite(parsed) ? parsed : null;
  }
  if (epoch == null && mode !== 'paused') epoch = saved?.epoch ?? Date.now();

  const unit = represents(l.represents);
  const calendarStart = l['calendar.start'] ?? l.calendar ?? null;
  const startDate = calendarStart && Number.isFinite(Date.parse(calendarStart))
    ? new Date(Date.parse(calendarStart)) : null;

  return {
    id: entry.id,
    family: entry.family,
    key: entry.key,
    topic: l.topic || `${TOPIC_ROOT}/${entry.family}/${entry.key}`,
    intervalMs,
    speed,
    origin,
    mode,
    epoch,
    represents: unit,
    calendarStart: startDate ? startDate.toISOString().slice(0, 10) : null,
    ticksPerYear: Number(l['calendar.ticksPerYear']) || null,
    label: l.label ?? null,
    source: entry.source,
    name: entry.name,
    startDate,
  };
}

/** The authoritative tick for a plan, at an instant. */
export function tickAt(plan, now = Date.now()) {
  if (plan.mode === 'paused' || plan.epoch == null) return plan.origin;
  const elapsed = (now - plan.epoch) * plan.speed;
  return plan.origin + Math.floor(elapsed / plan.intervalMs);
}

/**
 * What a tick number means on the family's own calendar, or null.
 *
 * Null rather than a throw, because the combination that overflows is easy to
 * write by accident: a fast interval, a month per tick, and an epoch some
 * months back multiply out to hundreds of thousands of years, which is past
 * what a Date can hold. The tick number itself is still perfectly good — only
 * its calendar rendering is not — so the family keeps ticking and loses a
 * cosmetic field.
 */
export function worldTimeOf(plan, tick) {
  if (!plan.startDate) return null;
  const d = advance(plan.startDate, plan.represents, tick - plan.origin);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString().replace('.000Z', 'Z');
}

/* ------------------------------------------------------ the service ------ */

export class Ticker {
  constructor({ client, topicRoot = TOPIC_ROOT, stateFile = STATE_FILE, dry = DRY } = {}) {
    this.client = client;
    this.topicRoot = topicRoot;
    this.stateFile = stateFile;
    this.dry = dry;
    this.plans = new Map();      // id -> plan
    this.timers = new Map();     // id -> timeout
    this.lastTick = new Map();   // id -> last published tick
    this.state = this.#loadState();
    this.published = 0;
  }

  #loadState() {
    try { return JSON.parse(fs.readFileSync(this.stateFile, 'utf8')); }
    catch { return { families: {} }; }
  }

  /**
   * Say once, at startup, whether state can be written — and what it costs.
   *
   * Found by deploying: a named volume is created root-owned, and this image
   * runs as `node`, so the very first write fails. That is harmless for
   * `start=epoch` families, whose epoch is in a label and needs no memory at
   * all, and quietly damaging for `start=now` ones, whose epoch lives nowhere
   * else and is therefore re-derived on every restart — a clock that silently
   * goes back to zero.
   *
   * So the difference is stated up front rather than left as a warning
   * somewhere in the log, and it names the fix.
   */
  checkState() {
    try {
      fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
      fs.accessSync(path.dirname(this.stateFile), fs.constants.W_OK);
      this.stateWritable = true;
    } catch {
      this.stateWritable = false;
      log('warn', `state is not writable at ${this.stateFile}`);
      log('warn', '  families using start=epoch are unaffected — their epoch is a label');
      log('warn', '  families using start=now will restart their clock on every restart');
      log('warn', `  fix: chown the volume to this user (${process.getuid?.() ?? '?'}:`
        + `${process.getgid?.() ?? '?'}), or set TICK_STATE to a writable path`);
    }
    return this.stateWritable;
  }

  #saveState() {
    if (this.stateWritable === false) return;      // already said so, once
    try {
      fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
      fs.writeFileSync(this.stateFile, JSON.stringify(this.state, null, 1));
    } catch (err) {
      this.stateWritable = false;
      log('warn', `cannot write ${this.stateFile}: ${err.message}`);
      log('warn', '  start=now families will restart their clock on every restart');
    }
  }

  /** Reconcile against a fresh discovery. */
  apply(entries) {
    const seen = new Set();
    for (const entry of entries) {
      seen.add(entry.id);
      try {
        this.#applyOne(entry);
      } catch (err) {
        // This service ticks several games at once. A container with labels
        // that do not make sense must cost that game its ticks and nobody
        // else's, so the failure is contained per family and named loudly
        // enough to fix.
        log('error', `family ${entry.id} rejected: ${err.message}`);
      }
    }

    for (const id of [...this.plans.keys()]) {
      if (seen.has(id)) continue;
      log('info', `family ${id} withdrawn`);
      clearTimeout(this.timers.get(id));
      this.timers.delete(id);
      this.plans.delete(id);
      this.lastTick.delete(id);
    }
  }

  #applyOne(entry) {
    {
      const saved = this.state.families[entry.id];
      const plan = planOf(entry, saved);

      // Persist the epoch only for families that cannot recompute it.
      if (plan.mode !== 'paused' && plan.epoch != null) {
        const before = saved?.epoch;
        this.state.families[entry.id] = { epoch: plan.epoch, family: plan.family, key: plan.key };
        if (before !== plan.epoch) this.#saveState();
      }

      const prev = this.plans.get(entry.id);
      const changed = !prev || JSON.stringify({ ...prev, startDate: null })
        !== JSON.stringify({ ...plan, startDate: null });
      this.plans.set(entry.id, plan);
      if (changed) {
        log('info', `family ${entry.id}: every ${plan.intervalMs}ms = `
          + `${plan.represents.count} ${plan.represents.unit}(s), ${plan.mode}`
          + `, -> ${plan.topic}`);
        this.#schedule(plan);
        this.publish(plan, 'config');
      }
    }
  }

  /**
   * Wake exactly when the next tick is due, rather than polling.
   *
   * The tick is derived from the clock, so the schedule only decides WHEN to
   * announce it. Aiming at the boundary keeps announcements aligned with the
   * numbers they carry even when timers fire late.
   */
  #schedule(plan) {
    clearTimeout(this.timers.get(plan.id));
    if (plan.mode === 'paused' || plan.epoch == null) return;

    const fire = () => {
      const current = this.plans.get(plan.id);
      if (!current) return;
      try { this.publish(current, 'tick'); }
      catch (err) { log('error', `family ${current.id} publish failed: ${err.message}`); }
      const now = Date.now();
      const tick = tickAt(current, now);
      // Time of the NEXT boundary, in real milliseconds.
      const nextAt = current.epoch
        + ((tick + 1 - current.origin) * current.intervalMs) / (current.speed || 1);
      const wait = Math.max(25, nextAt - now);
      const t = setTimeout(fire, wait);
      t.unref?.();
      this.timers.set(current.id, t);
    };
    const t = setTimeout(fire, 25);
    t.unref?.();
    this.timers.set(plan.id, t);
  }

  /**
   * Publish a family's state. Retained, always.
   *
   * Retention is what lets a client that connects at any moment know the time
   * immediately instead of waiting up to a whole interval — and for a family
   * ticking once an hour, that difference is the difference between usable and
   * not.
   */
  publish(plan, reason = 'tick') {
    const now = Date.now();
    const tick = tickAt(plan, now);
    const msg = {
      v: 1,
      family: plan.family,
      key: plan.key,
      tick,
      // Everything a client needs to compute the tick itself between messages,
      // which is what makes a dropped packet or a dead ticker survivable.
      epoch: plan.epoch,
      origin: plan.origin,
      intervalMs: plan.intervalMs,
      speed: plan.speed,
      mode: plan.mode,
      represents: plan.represents,
      calendarStart: plan.calendarStart,
      ticksPerYear: plan.ticksPerYear,
      worldTime: worldTimeOf(plan, tick),
      label: plan.label,
      ticker: ID,
      at: now,
      reason,
    };
    this.lastTick.set(plan.id, tick);
    this.published += 1;
    if (this.dry) { log('debug', `[dry] ${plan.topic} ${JSON.stringify(msg)}`); return msg; }
    this.client?.publish(plan.topic, JSON.stringify(msg), { retain: true });
    log('debug', `${plan.topic} tick ${tick}${msg.worldTime ? ` (${msg.worldTime})` : ''}`);
    return msg;
  }

  stop() {
    for (const t of this.timers.values()) clearTimeout(t);
    this.timers.clear();
  }
}

/* ------------------------------------------------------ entry point ------ */

async function main() {
  log('info', `ofm ticker ${ID}`);
  log('info', `broker  ${BROKER}`);
  log('info', `topics  ${TOPIC_ROOT}/<family>/<key>`);
  log('info', `state   ${STATE_FILE}`);

  const client = DRY ? null : new MqttClient({
    url: BROKER,
    clientId: `ofm-ticker-${ID}-${Math.random().toString(16).slice(2, 6)}`,
    username: env.MQTT_USERNAME || undefined,
    password: env.MQTT_PASSWORD || undefined,
  });
  client?.on('connect', () => log('info', 'broker connected'));
  client?.on('close', () => log('warn', 'broker disconnected'));
  client?.on('error', (e) => log('warn', `broker: ${e.message}`));
  client?.connect();

  const ticker = new Ticker({ client });
  ticker.checkState();

  if (!(await docker.available())) {
    log('error', 'docker socket unreachable — nothing to discover. '
      + 'Mount /var/run/docker.sock into this container.');
  }

  const rediscover = async () => {
    try {
      const entries = await docker.discover();
      ticker.apply(entries);
      log('debug', `${entries.length} tick famil${entries.length === 1 ? 'y' : 'ies'}`);
    } catch (err) {
      log('warn', `discovery failed: ${err.message}`);
    }
  };

  await rediscover();
  const unwatch = docker.watch(rediscover);
  const poll = setInterval(rediscover, POLL_MS);
  poll.unref?.();

  const bye = () => {
    log('info', 'stopping');
    unwatch();
    clearInterval(poll);
    ticker.stop();
    client?.end();
    process.exit(0);
  };
  process.on('SIGTERM', bye);
  process.on('SIGINT', bye);
}

// Only run when invoked directly, so the tests can import the pieces.
if (import.meta.url === `file://${process.argv[1]}`) main();
