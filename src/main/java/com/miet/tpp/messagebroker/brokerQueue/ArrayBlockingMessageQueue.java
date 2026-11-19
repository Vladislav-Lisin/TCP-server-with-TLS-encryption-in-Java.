package com.miet.tpp.messagebroker.brokerQueue;

import java.util.concurrent.ArrayBlockingQueue;

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

    public Message take() throws InterruptedException {
        while (!closed || !messages.isEmpty()) {
            Message message = messages.poll();

            if (message != null) {
                return message;
            }

            Thread.sleep(10);
        }

        return null;
    }

    public void close() {
        closed = true;
    }
}
