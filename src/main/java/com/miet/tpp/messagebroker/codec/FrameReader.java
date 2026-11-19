package com.miet.tpp.messagebroker.codec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * FrameReader
 */
public class FrameReader {
    private static final int MAX_FRAME_LENGTH = 1 << 20;

    private static final int CONNECT = 0x01;
    private static final int CONNACK = 0x02;
    private static final int SUBSCRIBE = 0x03;
    private static final int UNSUBSCRIBE = 0x04;
    private static final int PUBLISH = 0x05;
    private static final int MESSAGE = 0x06;
    private static final int ACK = 0x07;
    private static final int DISCONNECT = 0x08;
    private static final int CREATE_TOPIC = 0x09;
    private static final int LIST_TOPICS = 0x0A;
    private static final int TOPIC_LIST = 0x0B;

    private final InputStream in;

    public FrameReader(InputStream in) {
        this.in = in;
    }

    public Frame readFrame() throws IOException {
        byte[] lengthBytes = new byte[4];
        if (in.readNBytes(lengthBytes, 0, 4) < 4) {
            throw new EOFException("Соединение закрыто до начала кадра");
        }

        int length = ByteBuffer.wrap(lengthBytes).getInt();

        if (length < 1 || length > MAX_FRAME_LENGTH) {
            throw new IOException("Некорректная длина кадра: " + length);
        }

        int type = in.read();
        if (type < 0) {
            throw new EOFException("Соединение закрыто при чтении type");
        }

        byte[] payload = new byte[length - 1];
        if (in.readNBytes(payload, 0, payload.length) < payload.length) {
            throw new EOFException("Соединение закрыто при чтении payload");
        }

        return parse(type, payload);
    }

    private Frame parse(int type, byte[] payload) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        try {
            return switch (type) {
                case CONNECT -> new Frame.Connect(readString(buf));
                case CONNACK -> new Frame.ConnAck(buf.get());
                case SUBSCRIBE -> new Frame.Subscribe(buf.getInt(), readString(buf));
                case UNSUBSCRIBE -> new Frame.Unsubscribe(buf.getInt(), readString(buf));
                case PUBLISH -> new Frame.Publish(buf.getInt(), readString(buf), readRemaining(buf));
                case MESSAGE -> new Frame.Message(readString(buf), buf.getLong(), readString(buf), readRemaining(buf));
                case ACK -> new Frame.Ack(buf.getInt(), buf.get());
                case DISCONNECT -> new Frame.Disconnect();
                case CREATE_TOPIC -> new Frame.CreateTopic(buf.getInt(), readString(buf));
                case LIST_TOPICS -> new Frame.ListTopics(buf.getInt());
                case TOPIC_LIST -> {
                    int requestId = buf.getInt();
                    int count = buf.getInt();
                    if (count < 0 || count > buf.remaining() / 2) {
                        throw new IOException("Некорректное число топиков: " + count);
                    }
                    List<String> topics = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        topics.add(readString(buf));
                    }
                    yield new Frame.TopicList(requestId, List.copyOf(topics));
                }
                default -> throw new IOException("Неизвестный тип кадра: " + type);
            };
        } catch (BufferUnderflowException e) {
            throw new IOException("Обрыв или некорректный payload кадра type=" + type, e);
        }
    }

    private static String readString(ByteBuffer buf) {
        int len = buf.getShort() & 0xFFFF;
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] readRemaining(ByteBuffer buf) {
        byte[] body = new byte[buf.remaining()];
        buf.get(body);
        return body;
    }
}
