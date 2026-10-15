package com.hanyang.cs;

import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IntegrityAuditTest {
    @Test void normalSequenceDoesNotReportAnomalies() {
        KafkaBenchmark.IntegrityAudit a = new KafkaBenchmark.IntegrityAudit();
        for (int i = 0; i < 10; i++) a.accept(record(0, i, i, KafkaBenchmark.RUN_BYTES));
        assertEquals(0, a.examples);
        assertEquals(0, a.sequenceGapEvents);
        a.accept(record(0, 10, 12, KafkaBenchmark.RUN_BYTES));
        assertEquals(1, a.sequenceGapEvents);
    }
    private ConsumerRecord<String, byte[]> record(int partition, long offset,
                                                 long seq, byte[] run) {
        ConsumerRecord<String, byte[]> r = new ConsumerRecord<>("test", partition, offset, "0", new byte[0]);
        r.headers().add("benchmark-sequence", ByteBuffer.allocate(12).putInt(0).putLong(seq).array());
        if (run != null) r.headers().add("benchmark-run", run);
        return r;
    }

    @Test void detectsReplayBeyondAdjacentDuplicateAndKeepsHighWater() {
        KafkaBenchmark.IntegrityAudit a = new KafkaBenchmark.IntegrityAudit();
        a.accept(record(0, 10, 10, KafkaBenchmark.RUN_BYTES));
        a.accept(record(0, 11, 11, KafkaBenchmark.RUN_BYTES));
        a.accept(record(0, 5, 5, KafkaBenchmark.RUN_BYTES));
        a.accept(record(0, 6, 6, KafkaBenchmark.RUN_BYTES));
        assertEquals(2, a.offsetRegressions);
        assertEquals(2, a.sequenceRegressions);
        assertEquals(11L, a.offsets.get(0));
    }

    @Test void separatesNewOffsetSequenceReplayAndForeignRun() {
        KafkaBenchmark.IntegrityAudit a = new KafkaBenchmark.IntegrityAudit();
        a.accept(record(0, 10, 10, KafkaBenchmark.RUN_BYTES));
        a.accept(record(0, 11, 9, KafkaBenchmark.RUN_BYTES));
        a.accept(record(1, 0, 100, new byte[]{1}));
        a.accept(record(1, 1, 200, null));
        assertEquals(0, a.offsetRegressions);
        assertEquals(1, a.sequenceRegressions);
        assertEquals(1, a.foreignRun);
        assertEquals(1, a.missingRun);
        assertEquals(10L, a.sequences.get(0));
    }

    @Test void boundsExamplesButCountsAllEvents() {
        KafkaBenchmark.IntegrityAudit a = new KafkaBenchmark.IntegrityAudit();
        for (int i = 0; i < 100; i++) a.accept(record(0, i, i, null));
        assertEquals(64, a.examples);
        assertEquals(100, a.missingRun);
    }
}
