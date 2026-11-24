package com.miet.tpp.messagebroker.messages;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.miet.tpp.messagebroker.stats.BrokerStats;

/*
 * ReentrantRWBroker
 */
public class ReentrantRWBroker implements Broker {
    private final ReentrantReadWriteLock  lock = new ReentrantReadWriteLock();
    private final Map<String, Topic> topiksMap = new HashMap<>();
    private final BrokerStats stats;

    public ReentrantRWBroker(BrokerStats stats) {
        this.stats = stats;
    }

    public int creatTopic(String name){
        lock.writeLock().lock();
        try{
            if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("Invalid topic name");
            }
            if(topiksMap.containsKey(name)){
                return 4; // уже существует
            }
            topiksMap.put(name, new Topic(name));
            return 0;
        }finally{
            lock.writeLock().unlock();
        }
    }

    public Set<String> getListTopics(){
        lock.readLock().lock();
        try{
            return Set.copyOf(topiksMap.keySet());
        } finally{
            lock.readLock().unlock();
        }
    }

    public boolean hasTopic(String name){
        lock.readLock().lock();
        try{
            return topiksMap.containsKey(name);
        } finally{
            lock.readLock().unlock();
        }
    }

    public void unsubscribeAll(Client client){
        lock.writeLock().lock();
        try{
            for (Topic topic : topiksMap.values()) {
                topic.delSubscriber(client);
            }
        } finally{
            lock.writeLock().unlock();
        }
    }

    public int subscribeToTopic(String name, Client client){
        lock.writeLock().lock();
        try{
            if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("Invalid topic name");
            }
            if(!topiksMap.containsKey(name)){
                return 3; // топик не найден 
            }
            Topic topic = topiksMap.get(name);
            topic.addSubscriber(client);
            return 0;
        } finally{
            lock.writeLock().unlock();
        }
    }
    
    public int unsubscribeFromTopic(String name, Client client){
        lock.writeLock().lock();
        try{
            if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("Invalid topic name");
            }
            if(!topiksMap.containsKey(name)){
                return 3; // топик не найден
            }
            Topic topic = topiksMap.get(name);
            topic.delSubscriber(client);
            return 0;
        } finally{
            lock.writeLock().unlock();
        }
    }

    public int publish(Message message){
        String name = message.getTopic();
        lock.readLock().lock();
        try{
            if (name == null || !name.matches("[a-zA-Z0-9._-]{1,64}")) {
                return 2; // недопустимое имя топика
            }
            Topic topic = topiksMap.get(name);
            if(topic == null){
                return 3; // топик не найден
            }

            Message delivered = message.withSeq(topic.nextSeq());
            for(Client sub : topic.getSubscribers()){
                if (sub.addNewMessage(delivered)) {
                    stats.incDelivered(name);
                } else {
                    stats.incDropped(name);
                }
            }
            return 0;
        } finally{
            lock.readLock().unlock();
        }
    }
}
