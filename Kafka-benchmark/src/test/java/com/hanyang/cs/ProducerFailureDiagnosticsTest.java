package com.hanyang.cs;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;

class ProducerFailureDiagnosticsTest {
    @Test void logsAreBoundedAndSingleLine() {
        PrintStream original = System.err;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(bytes));
            for (int i = 0; i < 30; i++)
                ProducerFailureDiagnostics.record(2, "callback", new Exception("first\nsecond"));
        } finally { System.setErr(original); }
        String text = bytes.toString();
        assertEquals(16, text.lines().filter(s -> s.contains("class=java.lang.Exception")).count());
        assertTrue(text.contains("message=first second"));
        assertTrue(text.contains("이후 개별 예외 로그 생략"));
    }
}
