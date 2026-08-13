# ticker

[![ci](https://github.com/openfantasymap/ticker/actions/workflows/ci.yml/badge.svg)](https://github.com/openfantasymap/ticker/actions/workflows/ci.yml)
[![publish](https://github.com/openfantasymap/ticker/actions/workflows/publish.yml/badge.svg)](https://github.com/openfantasymap/ticker/actions/workflows/publish.yml)

One process that gives ticks to any number of games.

A shared world needs a shared clock, and a browser is a poor clock: it throttles
when backgrounded, it stops when closed, and electing one player's tab to keep
time makes the world only as reliable as whoever joined first. This service is
the central entity instead — and it knows nothing about any particular game.

```bash
docker run -d --name ticker \
  -e MQTT_URL=mqtt://broker:1883 \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  --group-add "$(stat -c '%g' /var/run/docker.sock)" \
  ghcr.io/openfantasymap/ticker:latest

# or straight from a checkout — there is nothing to install
node src/ticker.js
```

Then label any container that wants ticking, and it appears within a second.

## The split

**Environment says where.** One broker per deployment.

| variable | default | |
|---|---|---|
| `MQTT_URL` | `mqtt://localhost:1883` | `mqtt://` or `mqtts://` |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | — | if the broker wants them |
| `TICK_TOPIC_ROOT` | `ofm/tick` | topics are `<root>/<family>/<key>` |
| `TICK_STATE` | `/var/lib/ofm-ticker/state.json` | only `start=now` needs it |
| `TICK_DISCOVER_MS` | `30000` | backstop poll; Docker events drive it |
| `TICK_LOG` | `info` | `error` `warn` `info` `debug` |
| `TICK_DRY_RUN` | — | `true` logs instead of publishing |

**Labels say what a tick is, what it means, and how it starts.** They live on
the game's own container, so a game changes its own time without this service
being touched.

| label | default | |
|---|---|---|
| `ofm.tick.enable` | — | `true` to opt in. Nothing else is read without it |
| `ofm.tick.family` | `default` | class of tick: `cities-world`, `combat-round` |
| `ofm.tick.key` | container name | which world or table |
| `ofm.tick.topic` | `<root>/<family>/<key>` | explicit override |
| `ofm.tick.interval` | `60s` | **what a tick is**, in real time: `500ms` `30s` `5m` `2h` `1d` |
| `ofm.tick.represents` | `1 month` | **what it means**, in world time. Calendar units stay calendar units |
| `ofm.tick.calendar.start` | — | world date at `origin`, e.g. `2287-01-01` |
| `ofm.tick.calendar.ticksPerYear` | — | passed through to clients |
| `ofm.tick.start` | `epoch` | **how it starts**: `epoch`, `now`, `paused` |
| `ofm.tick.epoch` | — | the instant tick `origin` happened |
| `ofm.tick.origin` | `0` | tick number at the epoch |
| `ofm.tick.speed` | `1` | multiplier on real time |
| `ofm.tick.label` | — | human name, shown to players |

```yaml
labels:
  ofm.tick.enable: "true"
  ofm.tick.family: cities-world
  ofm.tick.key: toril
  ofm.tick.interval: 1h              # an hour of real time...
  ofm.tick.represents: 1 month       # ...is a month of Toril's
  ofm.tick.calendar.start: "2287-01-01"
  ofm.tick.start: epoch
  ofm.tick.epoch: "2026-01-01T00:00:00Z"
```

## Time is derived, not counted

A family with an epoch publishes

```
tick = origin + floor((now − epoch) × speed / interval)
```

so this process holds no authority it could lose. Restart it, run two of them,
or lose the broker for an hour: the tick afterwards is the tick that would have
been. The published message carries the **rules** as well as the number, so a
client keeps perfect time between messages and through an outage — the heartbeat
is a convenience, not a source of truth.

Only `start=now` needs the state file, because its epoch is the moment the
family was first seen and nothing else records it.

```json
{
  "v": 1, "family": "cities-world", "key": "toril",
  "tick": 5123, "epoch": 1767225600000, "origin": 0,
  "intervalMs": 3600000, "speed": 1, "mode": "epoch",
  "represents": { "unit": "month", "count": 1 },
  "calendarStart": "2287-01-01", "worldTime": "2713-12-01T00:00:00Z",
  "label": "Toril", "ticker": "ticker-1", "at": 1783680000000
}
```

Retained, always: a client connecting mid-hour learns the time immediately
instead of waiting an interval.

## Notes

- **No dependencies.** The Docker API is HTTP over a unix socket, and the MQTT
  the service needs is small enough to write. The image is node plus three files.
- **Mount the socket read-only** (`:ro`). It only ever lists and watches.
- **The image runs as `node`, not root**, so it needs the socket's group:
  `--group-add "$(stat -c '%g' /var/run/docker.sock)"`, or `group_add` in
  compose. Running as root instead would work and is not worth it. If you would
  rather not hand out the socket at all, put a read-only socket proxy in front
  and point `DOCKER_HOST_SOCKET` at it — this service only ever calls
  `/containers/json`, `/services` and `/events`.
- **One bad family cannot stop the others.** Labels that do not make sense cost
  that game its ticks and nobody else's.
- **QoS 0.** A tick is a retained heartbeat; a lost packet is superseded by the
  next one carrying the same authoritative number.
- Clients fall back gracefully: `cities` elects a browser to keep time when no
  service is publishing for its world, and hands authority back the moment one
  appears.

## Testing

```bash
npm test                              # needs a broker on localhost:1883
npm test -- --broker mqtt://host:1883
npm run test:offline                  # skips the protocol tests
```

CI stands up a mosquitto beside the job rather than pointing at a public broker,
because the MQTT client here is hand-written and "does it really speak the
protocol" is the one question worth failing a build over — while a public
broker's bad day is not.

## Releasing

Push a tag. `publish.yml` runs the tests, builds `linux/amd64` and `linux/arm64`,
pushes to `ghcr.io/openfantasymap/ticker`, attaches build provenance, and drafts
release notes.

```bash
git tag v0.1.0 && git push origin v0.1.0
```

`main` publishes `:edge`; a tag publishes `:1.2.3`, `:1.2`, `:1` and `:latest`.
