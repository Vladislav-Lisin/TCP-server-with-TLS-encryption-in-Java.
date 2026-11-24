package com.miet.tpp.messagebroker.stats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Статистика брокера: соединения, сообщения, счётчики по топикам.
 * По остановке пишется в stats.json.
 */
public final class BrokerStats {

    private final String counterMode;
    private final Counter connectionsAccepted;
    private final Counter connectionsRejected;
    private final Counter messagesPublished;
    private final Counter messagesDelivered;
    private final Counter messagesDropped;
    private final Map<String, TopicStats> topics = new ConcurrentHashMap<>();
    private final Map<String, String> config;
    private final long startedAt = System.currentTimeMillis();

    public BrokerStats(String counterMode, Map<String, String> config) {
        this.counterMode = counterMode;
        this.config = new TreeMap<>(config);
        this.connectionsAccepted = Counter.create(counterMode);
        this.connectionsRejected = Counter.create(counterMode);
        this.messagesPublished = Counter.create(counterMode);
        this.messagesDelivered = Counter.create(counterMode);
        this.messagesDropped = Counter.create(counterMode);
    }

    public void incAccepted() {
        connectionsAccepted.inc();
    }

    public void incRejected() {
        connectionsRejected.inc();
    }

    public void touchTopic(String topic) {
        topic(topic);
    }

    public void incPublished(String topic) {
        messagesPublished.inc();
        topic(topic).published.inc();
    }

    public void incDelivered(String topic) {
        messagesDelivered.inc();
        topic(topic).delivered.inc();
    }

    public void incDropped(String topic) {
        messagesDropped.inc();
        topic(topic).dropped.inc();
    }

    private TopicStats topic(String name) {
        return topics.computeIfAbsent(name, n -> new TopicStats(counterMode));
    }

    public void writeJson(Path path) throws IOException {
        long uptimeSeconds = Math.max(0, (System.currentTimeMillis() - startedAt) / 1000);
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"counterMode\": \"").append(escape(counterMode)).append("\",\n");
        sb.append("  \"connectionsAccepted\": ").append(connectionsAccepted.get()).append(",\n");
        sb.append("  \"connectionsRejected\": ").append(connectionsRejected.get()).append(",\n");
        sb.append("  \"messagesPublished\": ").append(messagesPublished.get()).append(",\n");
        sb.append("  \"messagesDelivered\": ").append(messagesDelivered.get()).append(",\n");
        sb.append("  \"messagesDropped\": ").append(messagesDropped.get()).append(",\n");
        sb.append("  \"uptimeSeconds\": ").append(uptimeSeconds).append(",\n");

        Map<String, TopicStats> sorted = new TreeMap<>(topics);
        sb.append("  \"topics\": {");
        boolean firstTopic = true;
        for (Map.Entry<String, TopicStats> e : sorted.entrySet()) {
            if (!firstTopic) {
                sb.append(",");
            }
            firstTopic = false;
            TopicStats t = e.getValue();
            sb.append("\n    \"").append(escape(e.getKey())).append("\": {")
                    .append("\"published\": ").append(t.published.get()).append(", ")
                    .append("\"delivered\": ").append(t.delivered.get()).append(", ")
                    .append("\"dropped\": ").append(t.dropped.get()).append("}");
        }
        if (!sorted.isEmpty()) {
            sb.append("\n  ");
        }
        sb.append("},\n");

        sb.append("  \"config\": {");
        boolean firstConfig = true;
        for (Map.Entry<String, String> e : config.entrySet()) {
            if (!firstConfig) {
                sb.append(",");
            }
            firstConfig = false;
            sb.append("\n    \"").append(escape(e.getKey())).append("\": \"")
                    .append(escape(e.getValue())).append("\"");
        }
        if (!config.isEmpty()) {
            sb.append("\n  ");
        }
        sb.append("}\n");
        sb.append("}\n");

        Files.writeString(path, sb.toString(), StandardCharsets.UTF_8);
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class TopicStats {
        private final Counter published;
        private final Counter delivered;
        private final Counter dropped;

        private TopicStats(String counterMode) {
            this.published = Counter.create(counterMode);
            this.delivered = Counter.create(counterMode);
            this.dropped = Counter.create(counterMode);
        }
    }
}
