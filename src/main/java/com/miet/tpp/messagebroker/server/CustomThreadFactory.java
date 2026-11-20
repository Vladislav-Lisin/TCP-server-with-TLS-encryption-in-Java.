package com.miet.tpp.messagebroker.server;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CustomThreadFactory
 */
public class CustomThreadFactory implements  ThreadFactory{

    private final String prefix;
        private final AtomicInteger counter = new AtomicInteger();

        CustomThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setUncaughtExceptionHandler((thread, ex) ->
                    System.err.println("[handler] поток " + thread.getName() + " упал: " + ex));
            return t;
        }

    
}
