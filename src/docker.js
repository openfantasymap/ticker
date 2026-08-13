// Reading tick configuration off Docker labels.
//
// The service is told WHERE to publish by the environment — one broker, one
// process — and told WHAT to publish by labels on whatever containers and
// services happen to be running. That split is the point: the broker is a
// property of the deployment, while a tick family is a property of the game
// that needs it, and games come and go without the ticker being redeployed.
//
// Both plain containers and Swarm services are read, because this host runs
// both and a game deployed either way should be tickable without knowing which.
//
// No dependencies: the Docker API is HTTP over a unix socket, and node can do
// that out of the box.

import http from 'node:http';

const SOCKET = process.env.DOCKER_HOST_SOCKET ?? '/var/run/docker.sock';
const PREFIX = 'ofm.tick.';

/** One request against the Docker API. `raw` skips JSON parsing. */
function api(path, { timeout = 8000, raw = false } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ socketPath: SOCKET, path, method: 'GET', timeout }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode >= 400) return reject(new Error(`docker ${res.statusCode}: ${body.slice(0, 200)}`));
        if (raw) return resolve(body);
        try { resolve(JSON.parse(body)); } catch (err) { reject(err); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('docker api timeout')));
    req.end();
  });
}

// `/_ping` answers with the plain text "OK", not JSON — parsing it as JSON made
// this report the socket as unreachable while every other call worked.
export const available = async () => {
  try { return (await api('/_ping', { raw: true })).trim() === 'OK'; } catch { return false; }
};

/** Just the ofm.tick.* labels, with the prefix stripped. */
function tickLabels(labels) {
  const out = {};
  for (const [k, v] of Object.entries(labels ?? {})) {
    if (k.startsWith(PREFIX)) out[k.slice(PREFIX.length)] = v;
  }
  return out;
}

/**
 * Every tick family currently declared, from containers and Swarm services.
 *
 * A container declares one family. Several containers may declare the SAME
 * family — a replicated service is the obvious case — and that must produce one
 * family, not N competing ones, so they are keyed and deduplicated here rather
 * than left for the caller to trip over.
 */
export async function discover() {
  const found = new Map();
  const filter = encodeURIComponent(JSON.stringify({ label: [`${PREFIX}enable=true`] }));

  const add = (labels, source, name) => {
    const l = tickLabels(labels);
    if (l.enable !== 'true') return;
    const family = l.family ?? 'default';
    const key = l.key ?? name ?? 'default';
    const id = `${family}/${key}`;
    // First declaration wins; replicas of one service are one family.
    if (!found.has(id)) found.set(id, { id, family, key, labels: l, source, name });
  };

  const results = await Promise.allSettled([
    api(`/containers/json?filters=${filter}`),
    // Swarm services carry their labels in two places: on the service itself and
    // on the container spec. Both are read, service-level first.
    api(`/services?filters=${filter}`),
  ]);

  const [containers, services] = results;
  if (containers.status === 'fulfilled') {
    for (const c of containers.value) {
      add(c.Labels, 'container', (c.Names?.[0] ?? '').replace(/^\//, ''));
    }
  }
  if (services.status === 'fulfilled') {
    for (const s of services.value) {
      const spec = s.Spec ?? {};
      add({ ...(spec.TaskTemplate?.ContainerSpec?.Labels ?? {}), ...(spec.Labels ?? {}) },
        'service', spec.Name);
    }
  }
  // If BOTH calls failed the caller should hear about it rather than silently
  // discovering nothing and reporting a healthy empty world.
  if (containers.status === 'rejected' && services.status === 'rejected') {
    throw new Error(`docker unreachable: ${containers.reason?.message ?? containers.reason}`);
  }
  return [...found.values()];
}

/**
 * Watch for changes, calling `onChange` when the set might have moved.
 *
 * Deliberately coarse: it does not try to diff events into family mutations,
 * it just says "something happened, look again". Rediscovery is two cheap API
 * calls, and a watcher that reasons about event payloads is a watcher with its
 * own bugs and its own drift from reality.
 */
export function watch(onChange, { debounceMs = 700 } = {}) {
  let timer = null, req = null, stopped = false;
  const bump = () => {
    clearTimeout(timer);
    timer = setTimeout(() => onChange(), debounceMs);
  };

  const open = () => {
    if (stopped) return;
    const filters = encodeURIComponent(JSON.stringify({
      type: ['container', 'service'],
      event: ['start', 'stop', 'die', 'destroy', 'update', 'create', 'remove'],
    }));
    req = http.request({
      socketPath: SOCKET, path: `/events?filters=${filters}`, method: 'GET',
    }, (res) => {
      res.setEncoding('utf8');
      res.on('data', () => bump());
      res.on('end', () => { if (!stopped) setTimeout(open, 2000); });
    });
    req.on('error', () => { if (!stopped) setTimeout(open, 5000); });
    req.end();
  };
  open();

  return () => { stopped = true; clearTimeout(timer); req?.destroy(); };
}
