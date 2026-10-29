package com.hanyang.cs;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProducerRetryPolicyTest {
    @Test void optInRetriesPreservesOrderingAndDefault() {
        String old = System.getProperty("benchmark.producerRetries");
        try {
            System.clearProperty("benchmark.producerRetries");
            Properties p = new Properties();
            KafkaBenchmark.configureProducerIntegrity(p);
            assertEquals("0", p.getProperty("retries"));
            System.setProperty("benchmark.producerRetries", "2147483647");
            KafkaBenchmark.configureProducerIntegrity(p);
            assertEquals("2147483647", p.getProperty("retries"));
            assertEquals("1", p.getProperty("max.in.flight.requests.per.connection"));
            assertEquals("false", p.getProperty("enable.idempotence"));
        } finally {
            if (old == null) System.clearProperty("benchmark.producerRetries");
            else System.setProperty("benchmark.producerRetries", old);
        }
    }
}
