// A small MQTT 3.1.1 client: connect, publish, subscribe, ping.
//
// The obvious move is `npm i mqtt`. This exists instead because the ticker is
// meant to be a thing you can drop next to anything — a scratch container, a
// systemd unit, a laptop — and a service with no dependencies has no install
// step, no lockfile, no audit noise, and a container image that is just
// node:alpine plus three files. The protocol surface actually needed here is
// small enough that writing it is cheaper than depending on it.
//
// What is implemented: CONNECT, PUBLISH (QoS 0, retain), SUBSCRIBE, PINGREQ,
// and enough of the inbound parser to read PUBLISH and the various acks.
//
// What is NOT: QoS 1 and 2, session resumption, TLS client certs, MQTT 5
// properties. A tick is a fire-and-forget heartbeat that is also RETAINED, so
// a lost packet costs nothing — the next one is a second away and carries the
// same authoritative number. If this ever needs delivery guarantees, that is
// the moment to take the dependency instead of growing this file.

import net from 'node:net';
import tls from 'node:tls';
import { EventEmitter } from 'node:events';

const CONNECT = 1, CONNACK = 2, PUBLISH = 3, SUBSCRIBE = 8, SUBACK = 9;
const PINGREQ = 12, PINGRESP = 13, DISCONNECT = 14;

/** MQTT encodes lengths in 7-bit groups with a continuation bit. */
function encodeLength(n) {
  const out = [];
  do {
    let byte = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) byte |= 0x80;
    out.push(byte);
  } while (n > 0);
  return Buffer.from(out);
}

function decodeLength(buf, offset) {
  let multiplier = 1, value = 0, i = offset, byte;
  do {
    if (i >= buf.length) return null;              // incomplete
    byte = buf[i++];
    value += (byte & 127) * multiplier;
    multiplier *= 128;
    if (multiplier > 128 ** 4) return null;        // malformed
  } while ((byte & 0x80) !== 0);
  return { value, bytes: i - offset };
}

const str = (s) => {
  const b = Buffer.from(s, 'utf8');
  const len = Buffer.alloc(2);
  len.writeUInt16BE(b.length);
  return Buffer.concat([len, b]);
};

export class MqttClient extends EventEmitter {
  /**
   * @param url        mqtt://host:1883 or mqtts://host:8883
   * @param clientId   must be unique on the broker
   * @param keepalive  seconds; the broker drops us if we go quiet for 1.5x this
   */
  constructor({ url, clientId, keepalive = 30, username, password, reconnectMs = 5000 } = {}) {
    super();
    this.url = new URL(url);
    this.clientId = clientId ?? `mqtt-min-${Math.random().toString(16).slice(2, 10)}`;
    this.keepalive = keepalive;
    this.username = username;
    this.password = password;
    this.reconnectMs = reconnectMs;
    this.connected = false;
    this.sock = null;
    this.buf = Buffer.alloc(0);
    this.packetId = 1;
    this.pending = [];          // publishes issued before the link came up
    this.closed = false;
    this.subs = new Set();      // resubscribed on every reconnect
  }

  connect() {
    if (this.sock || this.closed) return this;
    const secure = this.url.protocol === 'mqtts:' || this.url.protocol === 'ssl:';
    const port = Number(this.url.port) || (secure ? 8883 : 1883);
    const opts = { host: this.url.hostname, port };

    this.sock = secure ? tls.connect({ ...opts, servername: this.url.hostname }) : net.connect(opts);
    this.sock.setNoDelay(true);

    const onUp = () => this.#sendConnect();
    this.sock.once(secure ? 'secureConnect' : 'connect', onUp);
    this.sock.on('data', (d) => this.#onData(d));
    this.sock.on('error', (err) => this.emit('error', err));
    this.sock.on('close', () => this.#onClose());
    return this;
  }

  #sendConnect() {
    const flags = 0x02                                  // clean session
      | (this.username ? 0x80 : 0)
      | (this.password ? 0x40 : 0);
    const vh = Buffer.concat([
      str('MQTT'),
      Buffer.from([4, flags]),                          // protocol level 4, flags
      (() => { const b = Buffer.alloc(2); b.writeUInt16BE(this.keepalive); return b; })(),
    ]);
    const payload = [str(this.clientId)];
    if (this.username) payload.push(str(this.username));
    if (this.password) payload.push(str(this.password));
    this.#send(CONNECT, 0, Buffer.concat([vh, ...payload]));
  }

  #send(type, flags, body) {
    if (!this.sock || this.sock.destroyed) return false;
    const head = Buffer.from([(type << 4) | flags]);
    this.sock.write(Buffer.concat([head, encodeLength(body.length), body]));
    return true;
  }

  /** Publish. Retained by default is deliberate for ticks — see the header. */
  publish(topic, payload, { retain = false } = {}) {
    const body = Buffer.concat([
      str(topic),
      Buffer.from(typeof payload === 'string' ? payload : JSON.stringify(payload), 'utf8'),
    ]);
    if (!this.connected) {
      // Only the LAST value per topic is worth keeping: a tick that could not
      // be sent has already been superseded by the time the link returns.
      this.pending = this.pending.filter((p) => p.topic !== topic);
      this.pending.push({ topic, payload, retain });
      if (this.pending.length > 200) this.pending.shift();
      return false;
    }
    return this.#send(PUBLISH, retain ? 1 : 0, body);
  }

  subscribe(topic) {
    this.subs.add(topic);
    if (!this.connected) return false;
    const id = Buffer.alloc(2);
    id.writeUInt16BE(this.packetId++ & 0xffff);
    return this.#send(SUBSCRIBE, 2, Buffer.concat([id, str(topic), Buffer.from([0])]));
  }

  #onData(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    for (;;) {
      if (this.buf.length < 2) return;
      const type = this.buf[0] >> 4;
      const len = decodeLength(this.buf, 1);
      if (!len) return;
      const total = 1 + len.bytes + len.value;
      if (this.buf.length < total) return;                 // wait for the rest
      const body = this.buf.subarray(1 + len.bytes, total);
      this.buf = this.buf.subarray(total);
      this.#handle(type, this.buf0Flags ?? 0, body, this.buf);
    }
  }

  #handle(type, _flags, body) {
    switch (type) {
      case CONNACK: {
        const code = body[1];
        if (code !== 0) { this.emit('error', new Error(`connection refused, code ${code}`)); return; }
        this.connected = true;
        this.#startPings();
        for (const t of this.subs) this.subscribe(t);
        const queued = this.pending;
        this.pending = [];
        for (const p of queued) this.publish(p.topic, p.payload, { retain: p.retain });
        this.emit('connect');
        break;
      }
      case PUBLISH: {
        const tlen = body.readUInt16BE(0);
        const topic = body.subarray(2, 2 + tlen).toString('utf8');
        // QoS 0 only, so there is no packet id between topic and payload.
        const payload = body.subarray(2 + tlen).toString('utf8');
        this.emit('message', topic, payload);
        break;
      }
      case SUBACK: case PINGRESP: break;
      default: break;
    }
  }

  #startPings() {
    clearInterval(this.pinger);
    if (!this.keepalive) return;
    this.pinger = setInterval(() => {
      if (this.connected) this.#send(PINGREQ, 0, Buffer.alloc(0));
    }, this.keepalive * 500);                              // twice per keepalive
    this.pinger.unref?.();
  }

  #onClose() {
    const was = this.connected;
    this.connected = false;
    clearInterval(this.pinger);
    this.sock = null;
    this.buf = Buffer.alloc(0);
    if (was) this.emit('close');
    if (this.closed) return;
    // A ticker that gives up on its broker is a ticker that silently stops the
    // world, so reconnection is unconditional and forever.
    this.retry = setTimeout(() => this.connect(), this.reconnectMs);
    this.retry.unref?.();
  }

  end() {
    this.closed = true;
    clearInterval(this.pinger);
    clearTimeout(this.retry);
    if (this.sock && this.connected) { try { this.#send(DISCONNECT, 0, Buffer.alloc(0)); } catch { /* gone */ } }
    this.sock?.destroy();
    this.sock = null;
    this.connected = false;
  }
}
