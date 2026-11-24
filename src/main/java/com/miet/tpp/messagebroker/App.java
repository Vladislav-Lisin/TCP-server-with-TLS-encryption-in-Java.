package com.miet.tpp.messagebroker;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import com.miet.tpp.messagebroker.brokerQueue.ArrayBlockingMessageQueue;
import com.miet.tpp.messagebroker.brokerQueue.CustomBlockingQueue;
import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;
import com.miet.tpp.messagebroker.messages.Broker;
import com.miet.tpp.messagebroker.messages.ReentrantRWBroker;
import com.miet.tpp.messagebroker.messages.SynchronizedBroker;
import com.miet.tpp.messagebroker.server.Acceptor;
import com.miet.tpp.messagebroker.server.Dispatcher;
import com.miet.tpp.messagebroker.stats.BrokerStats;

public final class App {
    private static final int DEFAULT_PORT = 9000;
    private static final int DEFAULT_POOL_SIZE = 50;
    private static final int DEFAULT_EXECUTOR_QUEUE = 1000;
    private static final int DEFAULT_QUEUE_CAP = 1000;

    private App() {
    }

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        String queueType = "bq";
        String subs = "rw";
        String poolMode = "platform";
        int poolSize = DEFAULT_POOL_SIZE;
        String policy = "abort";
        int maxConn = 0;
        int queueCap = DEFAULT_QUEUE_CAP;
        String counters = "atomic";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--queue" -> queueType = args[++i];
                case "--subs" -> subs = args[++i];
                case "--pool" -> poolMode = args[++i];
                case "--pool-size" -> poolSize = Integer.parseInt(args[++i]);
                case "--reject" -> policy = args[++i];
                case "--max-conn" -> maxConn = Integer.parseInt(args[++i]);
                case "--queue-cap" -> queueCap = Integer.parseInt(args[++i]);
                case "--counters" -> counters = args[++i];
                default -> System.err.println("Неизвестный аргумент: " + args[i]);
            }
        }

        if ("virtual".equals(poolMode)) {
            System.out.println("Режим virtual: --pool-size/--reject не применяются; --queue-cap задаёт inputQueue.");
        }
        if (maxConn > 0) {
            System.out.println("Внимание: --max-conn (Semaphore) ещё не реализован (этап 2).");
        }

        Map<String, String> config = new LinkedHashMap<>();
        config.put("port", String.valueOf(port));
        config.put("queue", queueType);
        config.put("subs", subs);
        config.put("pool", poolMode);
        config.put("poolSize", String.valueOf(poolSize));
        config.put("reject", policy);
        config.put("maxConn", String.valueOf(maxConn));
        config.put("queueCap", String.valueOf(queueCap));
        config.put("counters", counters);

        BrokerStats stats = new BrokerStats(counters, config);

        Broker broker = "sync".equals(subs) ? new SynchronizedBroker(stats) : new ReentrantRWBroker(stats);
        IBlockingQueue inputQueue = switch (queueType) {
            case "wn" -> new CustomBlockingQueue(queueCap);
            default -> new ArrayBlockingMessageQueue(queueCap);
        };

        Acceptor acceptor = new Acceptor(port, poolSize, DEFAULT_EXECUTOR_QUEUE, policy, poolMode,
                broker, inputQueue, stats);
        Dispatcher dispatcher = new Dispatcher(inputQueue, broker);
        Thread dispatcherThread = new Thread(dispatcher, "dispatcher");
        dispatcherThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            acceptor.stop();
            inputQueue.close();
            try {
                dispatcherThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                stats.writeJson(Path.of("stats.json"));
                System.out.println("Статистика записана в stats.json");
            } catch (IOException e) {
                System.err.println("Не удалось записать stats.json: " + e.getMessage());
            }
        }, "shutdown"));

        System.out.println("messageBroker listening on port " + port);
        acceptor.start();
    }
}
