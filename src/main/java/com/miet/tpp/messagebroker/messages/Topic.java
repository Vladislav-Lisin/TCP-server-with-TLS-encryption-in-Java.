package com.miet.tpp.messagebroker.messages;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Topik
 */
public class Topic {
    private final String name;
    private final AtomicLong seq = new AtomicLong();
    private Set<Client> subscribersSet = new HashSet<>();
    
    public Topic(String name){
        this.name = name;
    }

    public long nextSeq(){
        return seq.incrementAndGet();
    }

    public String getName(){
        return name;
    }

    public Set<Client> getSubscribers(){
        return  Set.copyOf(subscribersSet);
    }

    public void addSubscriber(Client client){
        subscribersSet.add(client);
    }

    public void delSubscriber(Client client){
        subscribersSet.remove(client);
    }

}
