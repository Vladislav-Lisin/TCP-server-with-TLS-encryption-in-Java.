package com.miet.tpp.messagebroker.brokerQueue;

import com.miet.tpp.messagebroker.messages.Message;

/**
 * IBlockingQueue
 */
public interface IBlockingQueue {

    public void put(Message message) throws InterruptedException;
    /** Неблокирующая вставка. false — очередь заполнена. IllegalStateException — очередь закрыта. */
    public boolean offer(Message message);
    public Message take() throws InterruptedException;
    public void close();
}
