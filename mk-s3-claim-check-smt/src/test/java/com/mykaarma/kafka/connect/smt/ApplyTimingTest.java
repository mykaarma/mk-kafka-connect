package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ApplyTimingTest {

  private long now;
  private final List<String> lines = new ArrayList<>();

  private ApplyTiming timing(long windowMs) {
    return new ApplyTiming("x", windowMs, () -> now, lines::add);
  }

  private void call(ApplyTiming t, long nanos) {
    long s = t.start();
    now += nanos;
    t.end(s);
  }

  @Test
  public void logsOneSummaryWhenTheWindowEnds() {
    ApplyTiming t = timing(1000);
    for (int i = 0; i < 4; i++) {
      call(t, 1000);
    }
    assertTrue(lines.isEmpty());
    now += 1_000_000_000L;
    call(t, 9000);
    assertEquals(1, lines.size());
    assertTrue(lines.get(0), lines.get(0).contains("records=5"));
    assertTrue(lines.get(0), lines.get(0).contains("max_us=9.0"));
    assertTrue(lines.get(0), lines.get(0).contains("p50_us=1.0"));
  }

  @Test
  public void startsANewWindowAfterTheSummary() {
    ApplyTiming t = timing(1000);
    now += 2_000_000_000L;
    call(t, 1000);
    call(t, 1000);
    assertEquals(1, lines.size());
    now += 2_000_000_000L;
    call(t, 1000);
    assertEquals(2, lines.size());
    assertTrue(lines.get(1), lines.get(1).contains("records=2"));
  }

  @Test
  public void zeroWindowTurnsItOff() {
    assertFalse(timing(0).enabled());
  }
}
