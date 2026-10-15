package com.hanyang.cs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class KafkaBenchmarkTest {
    @Test void explicitAssignmentUsesAllEightPartitionsWithoutChangingLegacyDefault() {
        java.util.Set<Integer> explicit = new java.util.HashSet<>();
        java.util.Set<Integer> legacy = new java.util.HashSet<>();
        for (int id = 0; id < 8; id++) {
            explicit.add(KafkaBenchmark.producerPartition(id, 8));
            byte[] key = Integer.toString(id).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            legacy.add(org.apache.kafka.common.utils.Utils.toPositive(
                    org.apache.kafka.common.utils.Utils.murmur2(key)) % 8);
        }
        assertEquals(8, explicit.size());
        assertEquals(6, legacy.size());
        assertEquals(null, KafkaBenchmark.producerPartition(0, 0));
        assertThrows(IllegalArgumentException.class, () -> KafkaBenchmark.producerPartition(0, -1));
    }
    @Test
    void distributesTargetRateWithoutLosingRemainder() {
        assertArrayEquals(
                new int[] {2, 2, 2, 1, 1},
                KafkaBenchmark.distributeRate(8, 5));
    }

    @Test
    void supportsMoreProducersThanTargetOperations() {
        assertArrayEquals(
                new int[] {1, 1, 1, 0, 0},
                KafkaBenchmark.distributeRate(3, 5));
    }

    @Test
    void latencyCapacityIncludesOneSecondOfHeadroom() {
        assertEquals(61_024, KafkaBenchmark.latencyCapacity(1_000, 60));
        assertEquals(1_024, KafkaBenchmark.latencyCapacity(0, 60));
    }

    @Test
    void rejectsLatencyArraysLargerThanJvmLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () -> KafkaBenchmark.latencyCapacity(Integer.MAX_VALUE, 2));
    }

    @Test
    void countsSeparateAndConsecutiveAckStalls() {
        assertArrayEquals(
                new int[] {2, 5, 3},
                KafkaBenchmark.calculateAckStalls(new long[] {10, 0, 0, 4, 0, 0, 0, 8}));
    }

    @Test
    void strictSequenceOracleDisablesAmbiguousProducerRetries() {
        Properties props = new Properties();

        KafkaBenchmark.configureProducerIntegrity(props);

        assertEquals("false", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("1", props.getProperty(
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        assertEquals("0", props.getProperty(ProducerConfig.RETRIES_CONFIG));
    }

    @Test
    void failFastRequiresOnlySustainedMissingAcks() {
        long second = 1_000_000_000L;

        assertFalse(KafkaBenchmark.shouldFailFast(59 * second, 0, 60));
        assertFalse(KafkaBenchmark.shouldFailFast(600 * second, 0, 0));
        assertTrue(KafkaBenchmark.shouldFailFast(60 * second, 0, 60));
    }
}
