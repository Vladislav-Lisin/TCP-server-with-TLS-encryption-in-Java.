package com.miet.tpp.messagebroker.brokerQueue;

import com.miet.tpp.messagebroker.messages.Message;

/**
 * IBlockingQueue
 */
public interface IBlockingQueue {

    public void put(Message message) throws InterruptedException;
    public Message take() throws InterruptedException;
    public void close();
}
