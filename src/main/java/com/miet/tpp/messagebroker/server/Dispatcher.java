package com.miet.tpp.messagebroker.server;

import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;
import com.miet.tpp.messagebroker.messages.Broker;
import com.miet.tpp.messagebroker.messages.Message;

/**
 * Dispatcher
 */
public class Dispatcher implements Runnable {
    private final IBlockingQueue inputQueue;
    private final Broker broker;

    public Dispatcher(IBlockingQueue inputQueue, Broker broker) {
        this.inputQueue = inputQueue;
        this.broker = broker;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Message message = inputQueue.take();
                if (message == null) {
                    return; // очередь закрыта и опустошена
                }
                broker.publish(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
