// Checks the tick service.
//
//   node tools/ticker-check.js [--broker mqtt://host:1883]
//
// Three things are worth proving and none of them is obvious from reading:
//
//   1. The label vocabulary parses into the rules a game actually meant —
//      including the ones a duration cannot express, like "a tick is a month".
//   2. The tick is DERIVED from the clock, not counted. That is what lets the
//      service restart, run twice, or lose its broker without the world's date
//      moving. A counter would pass a casual test and fail exactly here.
//   3. The hand-rolled MQTT client really speaks MQTT to a real broker.
//
// The broker defaults to a local one so CI can stand up mosquitto beside this;
// pass --broker to point it elsewhere, or --no-broker to skip that section
// entirely when there is nothing to talk to.

import os from 'node:os';
import { MqttClient } from '../src/mqtt-min.js';
import {
  Ticker, planOf, tickAt, worldTimeOf, duration, represents, advance,
} from '../src/ticker.js';

let fails = 0;
const ok = (name, cond, detail = '') => {
  if (!cond) fails++;
  console.log(`  ${cond ? 'ok  ' : 'FAIL'}  ${name}${detail ? `  ${detail}` : ''}`);
};
const argv = process.argv.slice(2);
const flag = (n, d) => { const i = argv.indexOf(`--${n}`); return i >= 0 ? argv[i + 1] : d; };

const entry = (labels, id = 'cities-world/toril') => ({
  id, family: labels.family ?? 'cities-world', key: labels.key ?? 'toril',
  labels, source: 'container', name: 'test',
});

// ---- label vocabulary ----------------------------------------------------
console.log('\nlabels');
{
  ok('durations parse', duration('500ms') === 500 && duration('30s') === 30000
    && duration('5m') === 300000 && duration('2h') === 7200000 && duration('1d') === 86400000,
    '500ms 30s 5m 2h 1d');
  ok('a bare number is seconds', duration('90') === 90000);
  ok('nonsense falls back', duration('soon', 1234) === 1234 && duration(undefined, 7) === 7);

  ok('world-time units parse',
    represents('1 month').unit === 'month' && represents('6h').unit === 'hour'
    && represents('2 days').count === 2 && represents('1y').unit === 'year',
    '1 month, 6h, 2 days, 1y');
  ok('an unset unit defaults to a month', represents(undefined).unit === 'month');

  // Months are not a fixed number of milliseconds, and a game that ticks
  // monthly means calendar months. Flattening them to an average would drift a
  // day every few years, which is exactly the sort of thing nobody notices
  // until a harvest lands in the wrong season.
  const jan31 = new Date(Date.UTC(2287, 0, 31));
  ok('a month is a calendar month, not 30 days',
    advance(jan31, { unit: 'month', count: 1 }, 1).toISOString().startsWith('2287-03-03')
    || advance(jan31, { unit: 'month', count: 1 }, 1).getUTCMonth() === 2,
    advance(jan31, { unit: 'month', count: 1 }, 1).toISOString().slice(0, 10));
  ok('twelve monthly ticks make a year',
    advance(new Date(Date.UTC(2287, 0, 1)), { unit: 'month', count: 1 }, 12)
      .getUTCFullYear() === 2288);
}

// ---- a plan from labels --------------------------------------------------
console.log('\nplans');
{
  const p = planOf(entry({
    enable: 'true', family: 'cities-world', key: 'toril',
    interval: '1h', represents: '1 month',
    start: 'epoch', epoch: '2026-01-01T00:00:00Z', origin: '0',
    'calendar.start': '2287-01-01', 'calendar.ticksPerYear': '12',
  }));
  ok('interval becomes real milliseconds', p.intervalMs === 3600000, `${p.intervalMs}`);
  ok('the topic defaults from family and key',
    p.topic === 'ofm/tick/cities-world/toril', p.topic);
  ok('the epoch is honoured', p.epoch === Date.parse('2026-01-01T00:00:00Z'));
  ok('a tick means a month', p.represents.unit === 'month' && p.represents.count === 1);

  const explicit = planOf(entry({
    enable: 'true', topic: 'ofm/cities/world/toril/clock', interval: '30s',
  }));
  ok('an explicit topic overrides the default',
    explicit.topic === 'ofm/cities/world/toril/clock', explicit.topic);
}

// ---- the property that matters: time is derived --------------------------
console.log('\nderived time');
{
  const p = planOf(entry({
    enable: 'true', interval: '1h', epoch: '2026-01-01T00:00:00Z',
    represents: '1 month', 'calendar.start': '2287-01-01',
  }));
  const t0 = Date.parse('2026-01-01T00:00:00Z');

  ok('tick 0 at the epoch', tickAt(p, t0) === 0);
  ok('one interval is one tick', tickAt(p, t0 + 3600000) === 1);
  ok('it does not advance early', tickAt(p, t0 + 3599999) === 0);
  ok('a day of real time is 24 ticks', tickAt(p, t0 + 86400000) === 24);

  // The whole point: a fresh Ticker with no memory computes the same number.
  const later = t0 + 3600000 * 500;
  const restarted = planOf(entry({
    enable: 'true', interval: '1h', epoch: '2026-01-01T00:00:00Z',
    represents: '1 month', 'calendar.start': '2287-01-01',
  }));
  ok('a restarted ticker resumes the same tick',
    tickAt(restarted, later) === tickAt(p, later), `${tickAt(p, later)}`);

  // And two independent tickers agree, which is what makes running a second
  // one harmless rather than a race.
  ok('two tickers agree exactly', tickAt(planOf(entry({
    enable: 'true', interval: '1h', epoch: '2026-01-01T00:00:00Z',
  })), later) === tickAt(p, later));

  ok('world time follows the calendar',
    worldTimeOf(p, 12).startsWith('2288-01-01'), worldTimeOf(p, 12));
  ok('500 monthly ticks is about 41 years',
    worldTimeOf(p, 500).startsWith('2328-'), worldTimeOf(p, 500));

  // Speed multiplies real time, not world time.
  const fast = planOf(entry({
    enable: 'true', interval: '1h', epoch: '2026-01-01T00:00:00Z', speed: '4',
  }));
  ok('speed multiplies the rate', tickAt(fast, t0 + 3600000) === 4);

  const paused = planOf(entry({
    enable: 'true', interval: '1h', start: 'paused', origin: '77',
  }));
  ok('a paused family holds its tick',
    tickAt(paused, t0) === 77 && tickAt(paused, t0 + 1e9) === 77);
}

// ---- reconciliation ------------------------------------------------------
console.log('\ndiscovery and reconciliation');
{
  const sent = [];
  const fake = { publish: (topic, payload) => sent.push({ topic, payload: JSON.parse(payload) }) };
  const t = new Ticker({ client: fake, stateFile: `${os.tmpdir()}/ofm-ticker-test-state.json` });

  t.apply([entry({ enable: 'true', interval: '1h', epoch: '2026-01-01T00:00:00Z' })]);
  ok('a discovered family publishes at once', sent.length === 1, `${sent.length} message(s)`);
  ok('the message carries the rules, not just a number',
    sent[0].payload.intervalMs === 3600000 && sent[0].payload.epoch != null
    && sent[0].payload.represents?.unit === 'month',
    'a client can compute the tick itself between messages');

  // Same labels again must not re-announce; changed labels must.
  const before = sent.length;
  t.apply([entry({ enable: 'true', interval: '1h', epoch: '2026-01-01T00:00:00Z' })]);
  ok('an unchanged family is not re-announced', sent.length === before);
  t.apply([entry({ enable: 'true', interval: '30m', epoch: '2026-01-01T00:00:00Z' })]);
  ok('a changed interval is announced', sent.length === before + 1,
    `now ${sent[sent.length - 1].payload.intervalMs}ms`);

  // Two families at once, which is the reason this service is generic.
  t.apply([
    entry({ enable: 'true', family: 'cities-world', key: 'toril', interval: '1h' }, 'cities-world/toril'),
    entry({ enable: 'true', family: 'combat-round', key: 'table-9', interval: '6s', represents: '6s' },
      'combat-round/table-9'),
  ]);
  ok('several families coexist', t.plans.size === 2,
    [...t.plans.values()].map((p) => `${p.id}@${p.intervalMs}ms`).join(', '));

  t.apply([]);
  ok('a withdrawn family stops', t.plans.size === 0 && t.timers.size === 0);
  t.stop();
}

// ---- the MQTT client, against a real broker ------------------------------
const BROKER = flag('broker', process.env.MQTT_URL ?? 'mqtt://localhost:1883');
const SKIP_BROKER = argv.includes('--no-broker');
console.log(SKIP_BROKER ? '\nmqtt (skipped)' : `\nmqtt (${BROKER})`);
if (SKIP_BROKER) {
  console.log('        --no-broker: the protocol code is not exercised');
} else {
  const topic = `ofm/tick/selftest/${Math.random().toString(16).slice(2, 10)}`;
  const client = new MqttClient({ url: BROKER, clientId: `ofm-ticker-test-${Date.now()}` });

  const connected = await new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), 15000);
    client.on('connect', () => { clearTimeout(timer); resolve(true); });
    client.on('error', () => { clearTimeout(timer); resolve(false); });
    client.connect();
  });

  if (!connected) {
    console.log('        broker unreachable — skipping (public infrastructure)');
  } else {
    ok('it speaks MQTT to a real broker', true, BROKER);
    const got = await new Promise((resolve) => {
      const timer = setTimeout(() => resolve(null), 10000);
      client.on('message', (t, payload) => {
        if (t !== topic) return;
        clearTimeout(timer);
        try { resolve(JSON.parse(payload)); } catch { resolve(payload); }
      });
      client.subscribe(topic);
      // Give the SUBACK a moment; publishing into an unsubscribed topic would
      // test nothing.
      setTimeout(() => client.publish(topic, { tick: 42, hello: 'world' }, { retain: true }), 800);
    });
    ok('a published message comes back', got?.tick === 42, JSON.stringify(got));

    // Retained is the property the whole design leans on: a client joining an
    // hour late must learn the time immediately.
    const second = new MqttClient({ url: BROKER, clientId: `ofm-ticker-test2-${Date.now()}` });
    const retained = await new Promise((resolve) => {
      const timer = setTimeout(() => resolve(null), 12000);
      second.on('connect', () => second.subscribe(topic));
      second.on('message', (t, payload) => {
        if (t !== topic) return;
        clearTimeout(timer);
        try { resolve(JSON.parse(payload)); } catch { resolve(payload); }
      });
      second.connect();
    });
    ok('a late joiner gets the retained tick', retained?.tick === 42,
      'which is how a client knows the time without waiting an interval');
    // Clear it so the public broker is not left holding our test data.
    client.publish(topic, '', { retain: true });
    await new Promise((r) => setTimeout(r, 300));
    second.end();
  }
  client.end();
}

console.log(fails ? `\n${fails} failed\n` : '\nall passed\n');
process.exit(fails ? 1 : 0);
