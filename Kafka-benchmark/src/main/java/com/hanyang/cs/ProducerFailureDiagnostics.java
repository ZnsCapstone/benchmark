package com.hanyang.cs;

import java.util.concurrent.atomic.AtomicInteger;

/** 실패 정책을 바꾸지 않고 JVM별 최초 16건만 출력한다. */
final class ProducerFailureDiagnostics {
    private static final AtomicInteger COUNT = new AtomicInteger();
    static void record(int producer, String phase, Exception error) {
        int count = COUNT.incrementAndGet();
        if (count > 16) return;
        String message = String.valueOf(error.getMessage()).replace('\n', ' ').replace('\r', ' ');
        if (message.length() > 512) message = message.substring(0, 512);
        System.err.printf("[ProducerFailure] time=%s producer=%d phase=%s class=%s message=%s%n",
                java.time.Instant.now(), producer, phase, error.getClass().getName(), message);
        if (count == 16) System.err.println("[ProducerFailure] 이후 개별 예외 로그 생략; 실패 집계는 유지");
    }
}
