package com.miet.tpp.messagebroker.brokerQueue;



import com.miet.tpp.messagebroker.messages.Message;
/**
 * MyBrokerQueue
 */
public class CustomBlockingQueue implements IBlockingQueue{

    private final Message[] messages; 
    private int countMes = 0;
    private int newMesInd = 0;
    private int getMesInd = 0;
    private final int queueCap;
    private boolean closed = false;

    public CustomBlockingQueue(int queueCap){
        if (queueCap <= 0){
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.messages = new Message[queueCap];
        this.queueCap = queueCap;
    }

    public synchronized void put(Message message) throws InterruptedException{
        while(countMes == queueCap && !closed){
            wait();
        }
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }
        messages[newMesInd] =  message;
        newMesInd = (newMesInd + 1) % queueCap;
        countMes++;
        notifyAll();
    }

    public synchronized boolean offer(Message message) {
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }
        if (countMes == queueCap) {
            return false;
        }
        messages[newMesInd] = message;
        newMesInd = (newMesInd + 1) % queueCap;
        countMes++;
        notifyAll();
        return true;
    }

    public synchronized Message take() throws InterruptedException{
        while(countMes == 0 && !closed){
            wait();
        }

        if (countMes == 0 && closed) {
            return null;
        }
        Message mes = messages[getMesInd % queueCap];
        messages[getMesInd] = null;
        getMesInd = (getMesInd + 1) % queueCap;
        countMes--;
        notifyAll();
        return mes;        
    }

    public synchronized void close() {
        closed = true;
        notifyAll();
    }

}

