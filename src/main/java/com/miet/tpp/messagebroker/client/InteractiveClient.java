package com.miet.tpp.messagebroker.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import com.miet.tpp.messagebroker.codec.Frame;
import com.miet.tpp.messagebroker.codec.FrameReader;
import com.miet.tpp.messagebroker.codec.FrameWriter;

public final class InteractiveClient {
    private final Socket socket;
    private final FrameWriter writer;
    private final FrameReader reader;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private int nextRequestId = 1;

    public InteractiveClient(String host, int port, String clientId) throws IOException {
        this.socket = new Socket(host, port);
        this.writer = new FrameWriter(
                new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
        this.reader = new FrameReader(
                new BufferedInputStream(socket.getInputStream()));

        if (clientId != null) {
            writer.writeFrame(new Frame.Connect(clientId));
        }
    }

    private void start() {
        Thread readerThread = new Thread(this::readLoop, "client-reader");
        readerThread.start();

        System.out.println("Команды:");
        System.out.println("  connect <clientId>");
        System.out.println("  create <topic>");
        System.out.println("  topics            (алиас: list)");
        System.out.println("  sub <topic>       (алиас: subscribe)");
        System.out.println("  unsub <topic>     (алиас: unsubscribe)");
        System.out.println("  pub <topic> <body> (алиас: publish)");
        System.out.println("  quit              (алиас: disconnect / exit)");
        System.out.println();

        Scanner in = new Scanner(System.in, StandardCharsets.UTF_8);
        try {
            while (running.get() && in.hasNextLine()) {
                String line = in.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\\s+", 3);
                String cmd = parts[0].toLowerCase();
                try {
                    switch (cmd) {
                        case "connect" -> {
                            require(parts, 2, "connect <clientId>");
                            writer.writeFrame(new Frame.Connect(parts[1]));
                        }
                        case "create" -> {
                            require(parts, 2, "create <topic>");
                            writer.writeFrame(new Frame.CreateTopic(requestId(), parts[1]));
                        }
                        case "list", "topics" -> writer.writeFrame(new Frame.ListTopics(requestId()));
                        case "subscribe", "sub" -> {
                            require(parts, 2, "sub <topic>");
                            writer.writeFrame(new Frame.Subscribe(requestId(), parts[1]));
                        }
                        case "unsubscribe", "unsub" -> {
                            require(parts, 2, "unsub <topic>");
                            writer.writeFrame(new Frame.Unsubscribe(requestId(), parts[1]));
                        }
                        case "publish", "pub" -> {
                            require(parts, 3, "pub <topic> <body>");
                            writer.writeFrame(new Frame.Publish(requestId(), parts[1],
                                    parts[2].getBytes(StandardCharsets.UTF_8)));
                        }
                        case "disconnect", "exit", "quit" -> {
                            writer.writeFrame(new Frame.Disconnect());
                            Thread.sleep(300); // дать reader-потоку допечатать ответы
                            return;
                        }
                        default -> System.out.println("Неизвестная команда: " + cmd);
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println("Ошибка: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Ошибка записи: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            readerThread.interrupt();
            try {
                readerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private int requestId() {
        return nextRequestId++;
    }

    private static void require(String[] parts, int count, String usage) {
        if (parts.length < count) {
            throw new IllegalArgumentException("Использование: " + usage);
        }
    }

    private void readLoop() {
        try {
            while (running.get()) {
                Frame frame = reader.readFrame();
                System.out.println(format(frame));
            }
        } catch (IOException e) {
            if (running.get()) {
                System.out.println("Соединение закрыто сервером.");
                running.set(false);
            }
        }
    }

    private static String format(Frame frame) {
        return switch (frame) {
            case Frame.ConnAck c -> "ConnAck[status=" + c.status() + "]: " + connAckText(c.status());
            case Frame.Ack a -> "Ack[requestId=" + a.requestId() + ", status=" + a.status()
                    + "]: " + ackText(a.status());
            case Frame.Message m -> "Message[topic=" + m.topic() + ", seq=" + m.seq()
                    + ", senderId=" + m.senderId() + "]: "
                    + new String(m.body(), StandardCharsets.UTF_8);
            case Frame.TopicList t -> "TopicList[requestId=" + t.requestId() + "]: " + t.topics();
            default -> frame.toString();
        };
    }

    private static String ackText(byte status) {
        return switch (status) {
            case 0 -> "Успешно!";
            case 1 -> "Переполнение очереди";
            case 2 -> "Ошибка запроса";
            case 3 -> "Топик не найден";
            case 4 -> "Топик уже существует";
            default -> "Неизвестный статус";
        };
    }

    private static String connAckText(byte status) {
        return switch (status) {
            case 0 -> "Успешное подключение!";
            case 1 -> "Подключение отклонено";
            case 2 -> "Ошибка подключения";
            default -> "Неизвестный статус";
        };
    }

    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9000;
        String id = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--id" -> id = args[++i];
                default -> System.err.println("Неизвестный аргумент: " + args[i]);
            }
        }

        new InteractiveClient(host, port, id).start();
    }
}
