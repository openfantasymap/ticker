# examples

## A city builder world

An hour of real time is a month of world time, and the world's date is
reproducible because the epoch is fixed.

```yaml
labels:
  ofm.tick.enable: "true"
  ofm.tick.family: cities-world
  ofm.tick.key: toril
  ofm.tick.interval: 1h
  ofm.tick.represents: 1 month
  ofm.tick.calendar.start: "2287-01-01"
  ofm.tick.calendar.ticksPerYear: "12"
  ofm.tick.start: epoch
  ofm.tick.epoch: "2026-01-01T00:00:00Z"
  ofm.tick.label: Toril
```

## A combat round

Six seconds, starting the first time the table is seen. `start=now` is the one
mode that needs the state volume, because its epoch is not written anywhere else.

```yaml
labels:
  ofm.tick.enable: "true"
  ofm.tick.family: combat-round
  ofm.tick.key: table-9
  ofm.tick.interval: 6s
  ofm.tick.represents: 6 seconds
  ofm.tick.start: now
```

## A season, on a table that is not being played

`paused` publishes the rules and holds at `origin`, so clients still learn what
a tick means and the world simply does not move until the label changes.

```yaml
labels:
  ofm.tick.enable: "true"
  ofm.tick.family: season
  ofm.tick.key: greyhawk
  ofm.tick.interval: 1d
  ofm.tick.represents: 1 season
  ofm.tick.start: paused
  ofm.tick.origin: "3"
```

## Reading it

```bash
mosquitto_sub -h broker -t 'ofm/tick/#' -v
```

```js
// A client keeps time between messages from the rules, so a dropped packet or
// a restarted service costs nothing.
let rules = null;
client.on('message', (t, p) => { rules = JSON.parse(p); });

const tickNow = () => rules && rules.mode !== 'paused'
  ? rules.origin + Math.floor((Date.now() - rules.epoch) * rules.speed / rules.intervalMs)
  : rules?.origin ?? 0;
```
