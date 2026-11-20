import uuid

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from paperforge.events.bus import EventBus, EventConsumer, Message, Outgoing

MAX_MSG = 5 * 1024 * 1024


class KafkaConsumerImpl(EventConsumer):
    def __init__(self, conf: dict, topics: list[str]):
        self.c = Consumer(conf)
        self.c.subscribe(topics)

    def poll_batch(self, max_messages: int, timeout_s: float) -> list[Message]:
        out = []
        for m in self.c.consume(num_messages=max_messages, timeout=timeout_s):
            if m.error():
                if m.error().code() == -191:     # _PARTITION_EOF, informational
                    continue
                raise KafkaException(m.error())
            out.append(Message(m.topic(), m.partition(), m.offset(), m.key(), m.value(),
                               {k: v for k, v in (m.headers() or [])}))
        return out

    def commit(self, messages: list[Message]) -> None:
        if not messages:
            return
        hi: dict[tuple[str, int], int] = {}
        for m in messages:
            k = (m.topic, m.partition)
            hi[k] = max(hi.get(k, -1), m.offset)
        self.c.commit(offsets=[TopicPartition(t, p, o + 1) for (t, p), o in hi.items()], asynchronous=False)

    def close(self) -> None:
        self.c.close()


class KafkaEventBus(EventBus):
    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap
        self._producer: Producer | None = None

    @property
    def producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer({
                "bootstrap.servers": self.bootstrap, "acks": "all", "enable.idempotence": True,
                "compression.type": "zstd", "linger.ms": 50, "batch.size": 262144, "message.max.bytes": MAX_MSG,
                "queue.buffering.max.messages": 200000, "queue.buffering.max.kbytes": 524288,
            })
        return self._producer

    def publish(self, items: list[Outgoing]) -> list[Exception | None]:
        results: list[Exception | None] = [KafkaException("not delivered")] * len(items)
        p = self.producer

        def cb_for(i):
            def cb(err, _msg):
                results[i] = KafkaException(err) if err else None
            return cb

        for i, it in enumerate(items):
            while True:
                try:
                    p.produce(it.topic, key=it.key, value=it.value, headers=list(it.headers.items()) or None, on_delivery=cb_for(i))
                    break
                except BufferError:
                    p.poll(0.5)                  # local queue full: let deliveries drain
                except KafkaException as e:      # e.g. message too large: fail this item only
                    results[i] = e
                    break
            p.poll(0)
        p.flush(60)
        return results

    def consumer(self, group: str, topics: list[str]) -> EventConsumer:
        return KafkaConsumerImpl({
            "bootstrap.servers": self.bootstrap, "group.id": group, "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.partition.fetch.bytes": MAX_MSG, "fetch.max.bytes": 8 * MAX_MSG, "session.timeout.ms": 30000,
            "max.poll.interval.ms": 600000,
        }, topics)

    def ensure_topics(self, topics: dict[str, int]) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        existing = set(admin.list_topics(timeout=15).topics)
        new = [NewTopic(t, num_partitions=n, replication_factor=1,
                        config={"max.message.bytes": str(MAX_MSG), "retention.ms": str(14 * 24 * 3600 * 1000)})
               for t, n in topics.items() if t not in existing]
        for t, f in (admin.create_topics(new) if new else {}).items():
            f.result()

    def lag(self, group: str, topic: str) -> dict[int, tuple[int, int]]:
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group, "enable.auto.commit": False})
        try:
            parts = [TopicPartition(topic, p) for p in c.list_topics(topic, timeout=15).topics[topic].partitions]
            committed = {tp.partition: tp.offset for tp in c.committed(parts, timeout=15)}
            out = {}
            for tp in parts:
                _lo, hi = c.get_watermark_offsets(tp, timeout=15)
                off = committed.get(tp.partition, -1001)
                out[tp.partition] = (off if off >= 0 else -1, hi)
            return out
        finally:
            c.close()


def unique_group(prefix: str = "g") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
