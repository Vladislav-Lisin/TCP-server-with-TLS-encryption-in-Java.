package com.miet.tpp.messagebroker.codec;

import java.io.IOException;
import java.util.List;

import com.miet.tpp.messagebroker.messages.Client;
import com.miet.tpp.messagebroker.messages.Message;
import com.miet.tpp.messagebroker.server.Connection;

/**
 * Reader
 */
public class Reader implements Runnable {
    private static final String TOPIC_PATTERN = "[a-zA-Z0-9._-]{1,64}";

    private final Connection connection;

    public Reader(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void run() {
        try {
            while (!connection.isClosed()) {
                Frame frame = connection.getFrameReader().readFrame();
                if (!handle(frame)) {
                    return;
                }
            }
        } catch (IOException | RuntimeException e) {
            connection.close();
        }
    }

    private boolean handle(Frame frame) throws IOException {
        if (!(frame instanceof Frame.Connect) && connection.getClient() == null) {
            // первый кадр обязан быть CONNECT
            connection.close();
            return false;
        }
        switch (frame) {
            case Frame.Connect c -> {
                return connect(c);
            }
            case Frame.CreateTopic ct -> connection.send(
                    new Frame.Ack(ct.requestId(), createTopic(ct.topic())));
            case Frame.ListTopics lt -> connection.send(
                    new Frame.TopicList(lt.requestId(),
                            List.copyOf(connection.getBroker().getListTopics())));
            case Frame.Subscribe s -> connection.send(
                    new Frame.Ack(s.requestId(), subscribe(s.topic())));
            case Frame.Unsubscribe u -> connection.send(
                    new Frame.Ack(u.requestId(), unsubscribe(u.topic())));
            case Frame.Publish p -> {
                return publish(p);
            }
            case Frame.Disconnect d -> {
                connection.close();
                return false;
            }
            case Frame.ConnAck a -> throw new IOException("unexpected CONNACK from client");
            case Frame.Ack a -> throw new IOException("unexpected ACK from client");
            case Frame.Message m -> throw new IOException("unexpected MESSAGE from client");
            case Frame.TopicList tl -> throw new IOException("unexpected TOPIC_LIST from client");
        }
        return true;
    }

    private boolean connect(Frame.Connect c) throws IOException {
        if (connection.getClient() != null) {
            connection.send(new Frame.ConnAck((byte) 2));
            connection.close();
            return false;
        }

        String clientId = c.clientId();
        if (clientId == null || clientId.isBlank() || clientId.length() > 64) {
            connection.send(new Frame.ConnAck((byte) 2));
            connection.close();
            return false;
        }

        boolean initialized;
        try {
            initialized = connection.initClient(clientId);
        } catch (RuntimeException e) {
            // пул отклонил задачу Writer — сообщаем лимит соединений
            connection.send(new Frame.ConnAck((byte) 1));
            connection.close();
            return false;
        }
        if (!initialized) {
            connection.send(new Frame.ConnAck((byte) 2));
            connection.close();
            return false;
        }
        connection.getStats().incAccepted();
        connection.send(new Frame.ConnAck((byte) 0));
        return true;
    }

    private byte createTopic(String name) {
        try {
            int result = connection.getBroker().creatTopic(name);
            if (result == 0 || result == 4) {
                connection.getStats().touchTopic(name);
            }
            return (byte) result;
        } catch (IllegalArgumentException e) {
            return 2;
        }
    }

    private byte subscribe(String topic) {
        Client client = connection.getClient();
        if (client == null) {
            return 2;
        }
        try {
            return (byte) connection.getBroker().subscribeToTopic(topic, client);
        } catch (IllegalArgumentException e) {
            return 2;
        }
    }

    private byte unsubscribe(String topic) {
        Client client = connection.getClient();
        if (client == null) {
            return 2;
        }
        try {
            return (byte) connection.getBroker().unsubscribeFromTopic(topic, client);
        } catch (IllegalArgumentException e) {
            return 2;
        }
    }

    private boolean publish(Frame.Publish p) throws IOException {
        Client client = connection.getClient();
        if (client == null) {
            connection.close();
            return false;
        }

        String topic = p.topic();
        if (topic == null || !topic.matches(TOPIC_PATTERN)) {
            connection.send(new Frame.Ack(p.requestId(), (byte) 2));
            return true;
        }
        if (!connection.getBroker().hasTopic(topic)) {
            connection.send(new Frame.Ack(p.requestId(), (byte) 3));
            return true;
        }

        boolean accepted;
        try {
            accepted = connection.getInputQueue().offer(
                    new Message(topic, client.getId(), p.body(), p.requestId()));
        } catch (IllegalStateException e) {
            connection.close();
            return false;
        }

        if (!accepted) {
            connection.send(new Frame.Ack(p.requestId(), (byte) 1));
            return true;
        }

        connection.getStats().incPublished(topic);
        connection.send(new Frame.Ack(p.requestId(), (byte) 0));
        return true;
    }
}
