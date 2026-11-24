package com.miet.tpp.messagebroker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.miet.tpp.messagebroker.stats.BrokerStats;
import com.miet.tpp.messagebroker.stats.Counter;

class AppTest {

    @Test
    void countersIncrement() {
        for (String mode : new String[]{"volatile", "atomic", "adder"}) {
            Counter counter = Counter.create(mode);
            counter.inc();
            counter.inc();
            counter.inc();
            assertEquals(3, counter.get(), "режим " + mode);
        }
    }

    @Test
    void statsJsonContainsCounters(@TempDir Path dir) throws Exception {
        BrokerStats stats = new BrokerStats("atomic", Map.of("port", "9000"));
        stats.incAccepted();
        stats.touchTopic("news");
        stats.incPublished("news");
        stats.incDelivered("news");
        stats.incDropped("news");

        Path file = dir.resolve("stats.json");
        stats.writeJson(file);
        String json = Files.readString(file);

        assertTrue(json.contains("\"connectionsAccepted\": 1"), json);
        assertTrue(json.contains("\"messagesPublished\": 1"), json);
        assertTrue(json.contains("\"messagesDelivered\": 1"), json);
        assertTrue(json.contains("\"messagesDropped\": 1"), json);
        assertTrue(json.contains("\"news\""), json);
        assertTrue(json.contains("\"port\": \"9000\""), json);
    }
}
