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

    public void addNewMessage(Message message) throws InterruptedException {
        outgoingMessages.put(message);
    }

    public Message getNewMessage() throws InterruptedException {
        return outgoingMessages.take();
    }
    
}
