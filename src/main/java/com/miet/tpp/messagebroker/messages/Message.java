package com.miet.tpp.messagebroker.messages;

import java.util.Objects;

public final class Message {

    private final String topic;
    private final String senderId;
    private final byte[] body;
    private final int requestId;
    private final long seq;

    public Message(String topic, String senderId, byte[] body, int requestId) {
        this(topic, senderId, body, requestId, 0L);
    }

    public Message(String topic, String senderId, byte[] body, int requestId, long seq) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.senderId = Objects.requireNonNull(senderId, "senderId");
        this.body = body == null ? new byte[0] : body.clone();
        this.requestId = requestId;
        this.seq = seq;
    }

    public Message withSeq(long seq) {
        return new Message(topic, senderId, body, requestId, seq);
    }

    public String getTopic() {
        return topic;
    }

    public String getSenderId() {
        return senderId;
    }

    public byte[] getBody() {
        return body;
    }

    public int getRequestId() {
        return requestId;
    }

    public long getSeq() {
        return seq;
    }

    @Override
    public String toString() {
        return "Message{topic='%s', sender='%s', requestId=%d, seq=%d, body=%d bytes}"
                .formatted(topic, senderId, requestId, seq, body.length);
    }
}
