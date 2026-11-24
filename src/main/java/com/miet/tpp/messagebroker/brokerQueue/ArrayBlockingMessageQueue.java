package com.miet.tpp.messagebroker.brokerQueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.miet.tpp.messagebroker.messages.Message;

/**
 * MyBrokerQueue
 */
public class ArrayBlockingMessageQueue implements IBlockingQueue {

    private final ArrayBlockingQueue<Message> messages;
    private volatile boolean closed = false;

    public ArrayBlockingMessageQueue(int queueCap) {
        if (queueCap <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }

        this.messages = new ArrayBlockingQueue<>(queueCap);
    }

    public void put(Message message) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }

        messages.put(message);
    }

    public boolean offer(Message message) {
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }

        return messages.offer(message);
    }

    public Message take() throws InterruptedException {
        while (true) {
            Message message = messages.poll(100, TimeUnit.MILLISECONDS);
            if (message != null) {
                return message;
            }
            if (closed && messages.isEmpty()) {
                return null;
            }
        }
    }

    public void close() {
        closed = true;
    }
}
