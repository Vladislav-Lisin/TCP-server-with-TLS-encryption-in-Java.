package com.miet.tpp.messagebroker.messages;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.miet.tpp.messagebroker.stats.BrokerStats;

/**
 * SynchronizedBroker
 */
public class SynchronizedBroker implements Broker {
    private final Map<String, Topic> topicsMap = new HashMap<>();
    private final BrokerStats stats;

    public SynchronizedBroker(BrokerStats stats) {
        this.stats = stats;
    }

    public synchronized int creatTopic(String name) {
        if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Invalid topic name");
        }
        if (topicsMap.containsKey(name)) {
            return 4;
        }
        topicsMap.put(name, new Topic(name));
        return 0;
    }

    public synchronized Set<String> getListTopics() {
        return Set.copyOf(topicsMap.keySet());
    }

    public synchronized boolean hasTopic(String name) {
        return topicsMap.containsKey(name);
    }

    public synchronized int subscribeToTopic(String name, Client client) {
        if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Invalid topic name");
        }
        Topic topic = topicsMap.get(name);
        if (topic == null) {
            return 3;
        }
        topic.addSubscriber(client);
        return 0;
    }

    public synchronized int unsubscribeFromTopic(String name, Client client) {
        if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Invalid topic name");
        }
        Topic topic = topicsMap.get(name);
        if (topic == null) {
            return 3;
        }
        topic.delSubscriber(client);
        return 0;
    }

    public synchronized int publish(Message message) {
        String name = message.getTopic();
        if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
            return 2;
        }
        Topic topic = topicsMap.get(name);
        if (topic == null) {
            return 3;
        }

        Message delivered = message.withSeq(topic.nextSeq());
        for (Client sub : topic.getSubscribers()) {
            if (sub.addNewMessage(delivered)) {
                stats.incDelivered(name);
            } else {
                stats.incDropped(name);
            }
        }
        return 0;
    }

    public synchronized void unsubscribeAll(Client client) {
        for (Topic topic : topicsMap.values()) {
            topic.delSubscriber(client);
        }
    }
}
