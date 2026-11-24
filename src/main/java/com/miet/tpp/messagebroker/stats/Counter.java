package com.miet.tpp.messagebroker.stats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Счётчик с выбираемой стратегией (--counters volatile|atomic|adder).
 * Вариант volatile намеренно неатомарный — для эксперимента с потерей обновлений.
 */
public interface Counter {

    void inc();

    long get();

    static Counter create(String mode) {
        return switch (mode == null ? "" : mode) {
            case "volatile" -> new VolatileCounter();
            case "adder" -> new AdderCounter();
            default -> new AtomicCounter();
        };
    }

    final class AtomicCounter implements Counter {
        private final AtomicLong value = new AtomicLong();

        public void inc() {
            value.incrementAndGet();
        }

        public long get() {
            return value.get();
        }
    }

    final class AdderCounter implements Counter {
        private final LongAdder value = new LongAdder();

        public void inc() {
            value.increment();
        }

        public long get() {
            return value.sum();
        }
    }

    final class VolatileCounter implements Counter {
        private volatile long value;

        public void inc() {
            value++; // намеренно неатомарно
        }

        public long get() {
            return value;
        }
    }
}
