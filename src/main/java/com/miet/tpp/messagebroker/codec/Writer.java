package com.miet.tpp.messagebroker.codec;

import java.io.IOException;

import com.miet.tpp.messagebroker.messages.Client;
import com.miet.tpp.messagebroker.messages.Message;
import com.miet.tpp.messagebroker.server.Connection;

/**
 * Writer
 */
public class Writer implements Runnable {
    private final Connection connection;

    public Writer(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void run() {
        Client client = connection.getClient();
        try {
            while (!connection.isClosed()) {
                Message message = client.getNewMessage();
                if (message == null) {
                    return;
                }
                connection.send(new Frame.Message(
                        message.getTopic(),
                        message.getSeq(),
                        message.getSenderId(),
                        message.getBody()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            // соединение разорвано — закроем его ниже
        } finally {
            connection.close();
        }
    }
}
