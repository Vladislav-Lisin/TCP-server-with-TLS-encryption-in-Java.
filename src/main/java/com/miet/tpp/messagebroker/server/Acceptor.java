package com.miet.tpp.messagebroker.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.miet.tpp.messagebroker.brokerQueue.ArrayBlockingMessageQueue;
import com.miet.tpp.messagebroker.brokerQueue.CustomBlockingQueue;
import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;
import com.miet.tpp.messagebroker.codec.Frame;
import com.miet.tpp.messagebroker.messages.ReentrantRWBroker;

public class Acceptor {

    private final int port;
    private final int maxPoolSize;
    private final int queueCapacity;
    private final String policy;

    private final Set<Connection> activeConnections = ConcurrentHashMap.newKeySet();
    private final IBlockingQueue inputQueue;
    private final ReentrantRWBroker broker = new ReentrantRWBroker();

    private ServerSocket serverSocket;
    private ThreadPoolExecutor executor;
    private volatile boolean running = true;

    public Acceptor(int port, int maxPoolSize, int queueCapacity, String policy,
                    String queueType, int inputQueueCapacity) {
        this.port = port;
        this.maxPoolSize = maxPoolSize;
        this.queueCapacity = queueCapacity;
        this.policy = policy;

        this.inputQueue = switch (queueType) {
            case "wn" -> new CustomBlockingQueue(inputQueueCapacity);
            case "bq" -> new ArrayBlockingMessageQueue(inputQueueCapacity);
            default -> new CustomBlockingQueue(inputQueueCapacity);
        };
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            executor = new ThreadPoolExecutor(2, maxPoolSize,
                    60, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(queueCapacity),
                    new CustomThreadFactory("connection"),
                    new CustomRejectedExecutionHandler(policy));
        } catch (IOException ex) {
            System.out.println("Не удалось открыть порт " + port + ": " + ex.getMessage());
            return;
        }

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                Connection connection = new Connection(socket, broker, inputQueue);
                activeConnections.add(connection);

                try {
                    executor.execute(readerTask);
                } catch (RejectedExecutionException ex) {
                    try {
                        connection.send(new Frame.ConnAck((byte) 1));
                    } catch (IOException sendEx) {
                        // соединение всё равно закроется ниже
                    }
                    activeConnections.remove(connection);
                    connection.close();
                }
            } catch (IOException ex) {
                if (!running) {
                    break;
                }
                System.out.println("Ошибка при приёме соединения: " + ex.getMessage());
            }
        }
    }
}
