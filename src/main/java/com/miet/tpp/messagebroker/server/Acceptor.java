package com.miet.tpp.messagebroker.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;
import com.miet.tpp.messagebroker.codec.Frame;
import com.miet.tpp.messagebroker.messages.Broker;
import com.miet.tpp.messagebroker.stats.BrokerStats;

public class Acceptor {

    private final int port;
    private final int maxPoolSize;
    private final int queueCapacity;
    private final String policy;
    private final String poolMode;

    private final Set<Connection> activeConnections = ConcurrentHashMap.newKeySet();
    private final ClientRegistry clientRegistry = new ClientRegistry();
    private final Broker broker;
    private final IBlockingQueue inputQueue;
    private final BrokerStats stats;

    private ServerSocket serverSocket;
    private ExecutorService executor;
    private volatile boolean running = true;

    public Acceptor(int port, int maxPoolSize, int queueCapacity, String policy, String poolMode,
                    Broker broker, IBlockingQueue inputQueue, BrokerStats stats) {
        this.port = port;
        this.maxPoolSize = maxPoolSize;
        this.queueCapacity = queueCapacity;
        this.policy = policy;
        this.poolMode = poolMode;
        this.broker = broker;
        this.inputQueue = inputQueue;
        this.stats = stats;
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            executor = createExecutor();
        } catch (IOException ex) {
            System.out.println("Не удалось открыть порт " + port + ": " + ex.getMessage());
            return;
        }

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                Connection connection = new Connection(socket, broker, inputQueue,
                        clientRegistry, stats, executor, c -> activeConnections.remove(c));
                activeConnections.add(connection);

                try {
                    connection.start();
                } catch (RejectedExecutionException ex) {
                    stats.incRejected();
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

    public void stop() {
        running = false;
        closeServerSocket();
        for (Connection connection : activeConnections) {
            connection.close();
        }
        activeConnections.clear();
        shutdownExecutor();
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    private ExecutorService createExecutor() {
        if ("virtual".equals(poolMode)) {
            // один виртуальный поток на задачу (Reader/Writer каждого соединения)
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        ThreadPoolExecutor pool = new ThreadPoolExecutor(maxPoolSize, maxPoolSize,
                60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new CustomThreadFactory("connection"),
                new CustomRejectedExecutionHandler(policy));
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private void closeServerSocket() {
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void shutdownExecutor() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
