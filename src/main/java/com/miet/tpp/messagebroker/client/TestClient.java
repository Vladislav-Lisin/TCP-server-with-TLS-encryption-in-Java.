package com.miet.tpp.messagebroker.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import com.miet.tpp.messagebroker.codec.Frame;
import com.miet.tpp.messagebroker.codec.FrameReader;
import com.miet.tpp.messagebroker.codec.FrameWriter;

public final class TestClient {

    private TestClient() {
    }

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5555;

        try (Socket socket = new Socket(host, port)) {
            FrameWriter writer = new FrameWriter(
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
            FrameReader reader = new FrameReader(
                    new BufferedInputStream(socket.getInputStream()));

            writer.writeFrame(new Frame.Connect("alice"));
            System.out.println("CONNACK:      " + reader.readFrame());

            writer.writeFrame(new Frame.CreateTopic(1, "news"));
            System.out.println("CREATE_TOPIC: " + reader.readFrame());

            writer.writeFrame(new Frame.Subscribe(2, "news"));
            System.out.println("SUBSCRIBE:    " + reader.readFrame());

            writer.writeFrame(new Frame.Publish(3, "news",
                    "hello from alice".getBytes(StandardCharsets.UTF_8)));
            System.out.println("после PUBLISH (ожидаем ACK и MESSAGE):");
            for (int i = 0; i < 2; i++) {
                System.out.println("  frame: " + reader.readFrame());
            }

            writer.writeFrame(new Frame.Disconnect());
        }
    }
}
