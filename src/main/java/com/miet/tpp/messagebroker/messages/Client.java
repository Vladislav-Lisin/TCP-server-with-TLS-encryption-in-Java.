package com.miet.tpp.messagebroker.messages;

import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;

/**
 * Client
 */
public class Client {
    private final String clientId;
    private final IBlockingQueue outgoingMessages;

    public Client(String clientId, IBlockingQueue queue){
        this.clientId = clientId;
        this.outgoingMessages = queue;
    }

    public String getId(){
        return clientId;
    }

    /** false — исходящая очередь переполнена или клиент уже отключился. */
    public boolean addNewMessage(Message message) {
        try {
            return outgoingMessages.offer(message);
        } catch (IllegalStateException e) {
            return false;
        }
    }

    public Message getNewMessage() throws InterruptedException {
        return outgoingMessages.take();
    }
    
}
