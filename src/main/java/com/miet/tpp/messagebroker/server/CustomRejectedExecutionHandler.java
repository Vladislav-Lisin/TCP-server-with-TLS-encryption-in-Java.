package com.miet.tpp.messagebroker.server;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CustomRejectedExecutionHandler
 */
public class CustomRejectedExecutionHandler implements  RejectedExecutionHandler{
    private final String policy;
    private final AtomicLong rejectedCount = new AtomicLong(); 

    public CustomRejectedExecutionHandler(String policy){
        this.policy = policy;
    }

    @Override 
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor){
        long total = rejectedCount.incrementAndGet();
        System.err.println("Отклонена задача, mode=" + policy
                + ", queue=" + executor.getQueue().size()
                + ", active=" + executor.getActiveCount()
                + ", shutdown=" + executor.isShutdown()
                + ", всего отклонено: " + total);

        switch (policy) {
            case "abort" -> throw new RejectedExecutionException("Пул перегружен или остановлен");
            case "callerruns" -> {
                if (!executor.isShutdown()) {
                    r.run();            // выполняется в потоке, который вызвал execute (Acceptor)
                }
            }
            case "discard" -> { /* тихий отказ: задача просто выбрасывается */ }
            default -> throw new IllegalArgumentException("Неизвестная политика отказа: " + policy);
        }

    }

    
}
