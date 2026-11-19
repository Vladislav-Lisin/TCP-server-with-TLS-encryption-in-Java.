package com.miet.tpp.messagebroker.codec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * FrameWriter
 */
public class FrameWriter {

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
    private static final int MAX_FRAME_LENGTH = 1 << 20;



    private final DataOutputStream out;
    
    public FrameWriter(DataOutputStream out){
        this.out = out;
    }

    public void writeFrame(Frame frame) throws IOException{
        ByteArrayOutputStream payloadBuff = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(payloadBuff);

        int type = switch (frame) {
            case Frame.Connect c -> {
                writeString(p, c.clientId());
                yield CONNECT;
            }
            case Frame.ConnAck a -> {
                p.writeByte(a.status());
                yield CONNACK;
            }
            case Frame.Subscribe s -> {
                p.writeInt(s.requestId());
                writeString(p, s.topic());
                yield SUBSCRIBE;
            }
            case Frame.Unsubscribe u -> {
                p.writeInt(u.requestId());
                writeString(p, u.topic());
                yield UNSUBSCRIBE;
            }
            case Frame.Publish pub -> {
                p.writeInt(pub.requestId());
                writeString(p, pub.topic());
                p.write(pub.body());
                yield PUBLISH;
            }
            case Frame.Message m -> {
                writeString(p, m.topic());
                p.writeLong(m.seq());
                writeString(p, m.senderId());
                p.write(m.body());
                yield MESSAGE;
            }
            case Frame.Ack ack -> {
                p.writeInt(ack.requestId());
                p.writeByte(ack.status());
                yield ACK;
            }
            case Frame.Disconnect d -> {
                yield DISCONNECT;
            }
            case Frame.CreateTopic ct -> {
                p.writeInt(ct.requestId());
                writeString(p, ct.topic());
                yield CREATE_TOPIC;
            }
            case Frame.ListTopics lt -> {
                p.writeInt(lt.requestId());
                yield LIST_TOPICS;
            }
            case Frame.TopicList tl -> {
                p.writeInt(tl.requestId());
                p.writeInt(tl.topics().size());
                for (String topic : tl.topics()) {
                    writeString(p, topic);
                }
                yield TOPIC_LIST;
            }
        };
        byte[] payload = payloadBuff.toByteArray();

        if (1 + payload.length > MAX_FRAME_LENGTH) {
            throw new IOException("Кадр больше 1 МиБ: " + (1 + payload.length));
        }

        out.writeInt(1 + payload.length);
        out.writeByte(type);
        out.write(payload);
        out.flush();
    }
     private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            throw new IOException("Строка не может быть null");
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        if (bytes.length > 0xFFFF) {
            throw new IOException("Строка длиннее 65535 байт: " + bytes.length);
        }

        out.writeShort(bytes.length);
        out.write(bytes);
    }   
}
