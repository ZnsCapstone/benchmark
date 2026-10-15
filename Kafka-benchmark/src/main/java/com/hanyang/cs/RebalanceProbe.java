package com.hanyang.cs;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

/** Opt-in real-broker probe. Only writes/deletes records in topics it creates.
 * Unsubscribe/resubscribe deliberately loses assignment; it is NOT an exact
 * reproduction of a metadata-triggered rebalance retaining the same partitions.
 */
public final class RebalanceProbe {
    static final int PARTITIONS = 2, RECORDS = 100, PREFIX = 10;
    static final Duration POLL = Duration.ofMillis(200);

    static Properties base(String broker) {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        p.put("request.timeout.ms", "10000");
        p.put("default.api.timeout.ms", "20000");
        return p;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !args[1].equals("--allow-temp-topics")) {
            throw new IllegalArgumentException("Usage: RebalanceProbe host:port --allow-temp-topics");
        }
        String run = UUID.randomUUID().toString();
        System.out.println("[Probe] run=" + run + " broker=" + args[0]
                + " trigger=unsubscribe/resubscribe; topics retained; no device operations");
        try (AdminClient admin = AdminClient.create(base(args[0]))) {
            // A failed connection/create is an error, never a skipped/pass result.
            for (String mode : List.of("legacy", "fixed", "expired")) {
                String topic = "rebalance-probe-" + run + "-" + mode;
                admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1)
                        .configs(Map.of("cleanup.policy", "delete", "retention.ms", "86400000"))))
                        .all().get(30, TimeUnit.SECONDS);
                System.out.println("[Probe] created=" + topic);
                seed(args[0], topic, run);
                probe(admin, args[0], topic, run, mode);
            }
        }
        System.out.println("[Probe] PASS all cases; DM integrity/endurance NOT validated");
    }

    static void seed(String broker, String topic, String run) throws Exception {
        Properties p = base(broker);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("acks", "all");
        p.put("max.block.ms", "20000");
        p.put("delivery.timeout.ms", "30000");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int part = 0; part < PARTITIONS; part++) {
                for (int n = 0; n < RECORDS; n++) {
                    RecordMetadata m = producer.send(new ProducerRecord<>(topic, part,
                            "key", payload(run, part, n))).get(30, TimeUnit.SECONDS);
                    require(m.offset() == n, "unexpected seed offset " + m.offset());
                }
            }
        }
    }

    static String payload(String run, int partition, long offset) {
        return run + ":" + partition + ":" + offset;
    }

    static void probe(AdminClient admin, String broker, String topic, String run, String mode)
            throws Exception {
        boolean fixed = !mode.equals("legacy");
        Properties p = base(broker);
        p.put("group.id", topic + "-group");
        p.put("group.protocol", "classic");
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", fixed ? "none" : "earliest");
        p.put("max.poll.records", "1");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            ConsumerProgress progress = new ConsumerProgress(c);
            int[] events = new int[2];
            ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    events[0]++;
                    System.out.println("[Probe] mode=" + mode + " revoked=" + partitions);
                    if (fixed) progress.onPartitionsRevoked(partitions);
                }
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    events[1]++;
                    System.out.println("[Probe] mode=" + mode + " assigned=" + partitions);
                    if (fixed) progress.onPartitionsAssigned(partitions);
                }
            };
            c.subscribe(List.of(topic), listener);
            long[] expected = new long[PARTITIONS];
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (Arrays.stream(expected).anyMatch(n -> n < PREFIX)) {
                require(System.nanoTime() < deadline, "initial consumption timeout");
                for (ConsumerRecord<String, String> r : c.poll(POLL)) {
                    verify(r, run, expected);
                    if (fixed) progress.processed(r);
                    if (expected[r.partition()] == PREFIX)
                        c.pause(List.of(new TopicPartition(topic, r.partition())));
                }
            }
            Set<TopicPartition> partitions = new HashSet<>(c.assignment());
            require(partitions.size() == PARTITIONS, "missing initial assignment");
            for (OffsetAndMetadata committed : c.committed(partitions).values())
                require(committed == null, "unexpected committed offset");
            System.out.println("[Probe] mode=" + mode + " processed-next=" + Arrays.toString(expected)
                    + " committed=none");
            int oldAssignments = events[1];
            c.unsubscribe();
            require(c.assignment().isEmpty(), "unsubscribe did not revoke assignment");

            if (mode.equals("expired")) {
                // Deterministically advance log-start (retention-equivalent),
                // ONLY on this invocation's newly created test topic.
                Map<TopicPartition, RecordsToDelete> deletions = new HashMap<>();
                for (TopicPartition tp : partitions)
                    deletions.put(tp, RecordsToDelete.beforeOffset(PREFIX + 1));
                admin.deleteRecords(deletions).all().get(30, TimeUnit.SECONDS);
            }
            c.subscribe(List.of(topic), listener);
            if (mode.equals("legacy")) Arrays.fill(expected, 0);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            boolean first = true;
            try {
                while (Arrays.stream(expected).anyMatch(n -> n < RECORDS)) {
                    require(System.nanoTime() < deadline, "post-reassignment timeout");
                    ConsumerRecords<String, String> records = c.poll(POLL);
                    if (fixed) progress.checkFailure();
                    for (ConsumerRecord<String, String> r : records) {
                        if (first) {
                            System.out.println("[Probe] mode=" + mode + " first-resumed="
                                    + r.partition() + ":" + r.offset());
                            first = false;
                        }
                        require(!mode.equals("expired"), "expired case silently resumed");
                        verify(r, run, expected);
                        if (fixed) progress.processed(r);
                    }
                }
            } catch (RuntimeException e) {
                if (!mode.equals("expired")) throw e;
                // Accept only the production guard's specific failure, not
                // an unrelated broker timeout/disconnect/deserialization error.
                try { progress.checkFailure(); }
                catch (IllegalStateException guard) {
                    if (guard.getMessage().startsWith("consumer resume outside retained range:")) {
                        require(events[1] > oldAssignments && events[0] > 0, "no reassignment");
                        System.out.println("[Probe] PASS expired explicit range failure: " + guard.getMessage());
                        return;
                    }
                }
                throw e;
            }
            require(events[1] > oldAssignments && events[0] > 0, "no reassignment");
            require(!mode.equals("expired"), "expired case did not fail");
            System.out.println("[Probe] PASS " + mode + (fixed
                    ? " exact continuation, payload verified" : " legacy replay reproduced")
                    + " end=" + Arrays.toString(expected));
        }
    }

    static void verify(ConsumerRecord<String, String> r, String run, long[] expected) {
        require(r.offset() == expected[r.partition()], "offset mismatch partition=" + r.partition()
                + " expected=" + expected[r.partition()] + " actual=" + r.offset());
        require(payload(run, r.partition(), r.offset()).equals(r.value()), "payload mismatch");
        expected[r.partition()]++;
    }

    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
