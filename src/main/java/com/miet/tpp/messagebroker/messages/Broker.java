package com.miet.tpp.messagebroker.messages;

import java.util.Set;

public interface Broker {
    int creatTopic(String name);

    Set<String> getListTopics();

    boolean hasTopic(String name);

    int subscribeToTopic(String name, Client client);

    int unsubscribeFromTopic(String name, Client client);

    int publish(Message message);

    void unsubscribeAll(Client client);
}
