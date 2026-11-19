package com.miet.tpp.messagebroker.codec;

/**
 * Frame
 */
import java.util.List;

public sealed interface Frame {

    record Connect(String clientId) implements Frame {}

    record ConnAck(byte status) implements Frame {}

    record Subscribe(int requestId, String topic) implements Frame {}

    record Unsubscribe(int requestId, String topic) implements Frame {}

    record Publish(int requestId, String topic, byte[] body) implements Frame {}

    record Message(String topic, long seq, String senderId, byte[] body) implements Frame {}

    record Ack(int requestId, byte status) implements Frame {}

    record Disconnect() implements Frame {}

    record CreateTopic(int requestId, String topic) implements Frame {}

    record ListTopics(int requestId) implements Frame {}

    record TopicList(int requestId, List<String> topics) implements Frame {}
}
