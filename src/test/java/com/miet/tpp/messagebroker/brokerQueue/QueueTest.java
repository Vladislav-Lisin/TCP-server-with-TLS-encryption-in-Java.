package com.miet.tpp.messagebroker.brokerQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.miet.tpp.messagebroker.messages.Message;

class QueueTest {

    private static Message msg(String body) {
        return new Message("t", "sender", body.getBytes(StandardCharsets.UTF_8), 0);
    }

    @Test
    void arrayQueueOfferFullAndDrain() throws Exception {
        IBlockingQueue q = new ArrayBlockingMessageQueue(2);
        assertTrue(q.offer(msg("a")));
        assertTrue(q.offer(msg("b")));
        assertFalse(q.offer(msg("c")), "третье сообщение не должно влезть");

        assertEquals("a", body(q.take()));
        q.close();
        assertEquals("b", body(q.take()));
        assertNull(q.take());
        assertThrows(IllegalStateException.class, () -> q.offer(msg("d")));
    }

    @Test
    void customQueueOfferFullAndDrain() throws Exception {
        IBlockingQueue q = new CustomBlockingQueue(2);
        assertTrue(q.offer(msg("a")));
        assertTrue(q.offer(msg("b")));
        assertFalse(q.offer(msg("c")), "третье сообщение не должно влезть");

        assertEquals("a", body(q.take()));
        q.close();
        assertEquals("b", body(q.take()));
        assertNull(q.take());
        assertThrows(IllegalStateException.class, () -> q.offer(msg("d")));
    }

    @Test
    void customQueuePutBlocksUntilSpace() throws Exception {
        IBlockingQueue q = new CustomBlockingQueue(1);
        q.offer(msg("a"));
        Thread producer = new Thread(() -> {
            try {
                q.put(msg("b"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();
        Thread.sleep(100);
        assertTrue(producer.isAlive(), "put должен ждать освобождения места");
        assertEquals("a", body(q.take()));
        producer.join(1000);
        assertFalse(producer.isAlive());
        q.close();
    }

    private static String body(Message m) {
        return new String(m.getBody(), StandardCharsets.UTF_8);
    }
}
