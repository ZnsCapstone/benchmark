package com.hanyang.cs;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConsumerProgressTest {
    @Test void latestIsOnlyInitialPositionAndSignalsReadiness() {
        try (var c = mock()) {
            int[] assigned = {0};
            ConsumerProgress p = new ConsumerProgress(c, true, () -> assigned[0]++);
            p.onPartitionsAssigned(List.of(tp));
            assertEquals(300L, c.position(tp));
            assertEquals(1, assigned[0]);
            p.processed(new ConsumerRecord<>("test", 0, 301, "k", new byte[0]));
            c.updateEndOffsets(Map.of(tp, 400L));
            p.onPartitionsAssigned(List.of(tp));
            assertEquals(302L, c.position(tp));
        }
    }
    private final TopicPartition tp = new TopicPartition("test", 0);

    private MockConsumer<String, byte[]> mock() {
        MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        c.updateBeginningOffsets(Map.of(tp, 100L));
        c.updateEndOffsets(Map.of(tp, 300L));
        c.assign(List.of(tp));
        return c;
    }

    @Test void legacyLostPositionWithoutCommitFallsBackToBeginning() {
        try (var c = mock()) {
            c.seek(tp, 201);
            c.assign(List.of());
            c.assign(List.of(tp));
            assertEquals(100L, c.position(tp));
        }
    }

    @Test void restoreProcessedOffsetNotFetchPosition() {
        try (var c = mock()) {
            ConsumerProgress p = new ConsumerProgress(c);
            p.onPartitionsAssigned(List.of(tp));
            assertEquals(100L, c.position(tp));
            p.processed(new ConsumerRecord<>("test", 0, 200, "k", new byte[0]));
            c.seek(tp, 250); // fetched beyond the processed record
            p.onPartitionsRevoked(List.of(tp));
            c.assign(List.of());
            c.assign(List.of(tp));
            p.onPartitionsAssigned(List.of(tp));
            assertEquals(201L, c.position(tp));
            // A replay is still verified by the caller; it cannot lower progress.
            p.processed(new ConsumerRecord<>("test", 0, 150, "k", new byte[0]));
            p.onPartitionsLost(List.of(tp));
            p.onPartitionsAssigned(List.of(tp));
            assertEquals(201L, c.position(tp));
        }
    }

    @Test void retentionLossAndTruncationFailClosed() {
        for (boolean retention : List.of(true, false)) {
            try (var c = mock()) {
                ConsumerProgress p = new ConsumerProgress(c);
                p.processed(new ConsumerRecord<>("test", 0, 200, "k", new byte[0]));
                if (retention) c.updateBeginningOffsets(Map.of(tp, 202L));
                else c.updateEndOffsets(Map.of(tp, 200L));
                assertThrows(IllegalStateException.class, () -> p.onPartitionsAssigned(List.of(tp)));
                assertThrows(IllegalStateException.class, p::checkFailure);
            }
        }
    }
}
