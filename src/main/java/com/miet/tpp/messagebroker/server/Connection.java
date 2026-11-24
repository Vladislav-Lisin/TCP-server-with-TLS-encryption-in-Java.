package com.miet.tpp.messagebroker.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.miet.tpp.messagebroker.brokerQueue.ArrayBlockingMessageQueue;
import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;
import com.miet.tpp.messagebroker.codec.Frame;
import com.miet.tpp.messagebroker.codec.FrameReader;
import com.miet.tpp.messagebroker.codec.FrameWriter;
import com.miet.tpp.messagebroker.codec.Reader;
import com.miet.tpp.messagebroker.codec.Writer;
import com.miet.tpp.messagebroker.messages.Broker;
import com.miet.tpp.messagebroker.messages.Client;
import com.miet.tpp.messagebroker.stats.BrokerStats;

/**
 * Connection
 */
public class Connection {
    private static final int OUTGOING_QUEUE_CAPACITY = 1024;

    private final Socket socket;
    private final FrameReader frameReader;
    private final FrameWriter frameWriter;
    private final Broker broker;
    private final IBlockingQueue inputQueue;
    private final ClientRegistry clientRegistry;
    private final BrokerStats stats;
    private final Executor executor;
    private final Consumer<Connection> onClose;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Client client;
    private volatile IBlockingQueue outgoingMessages;

    public Connection(Socket socket, Broker broker, IBlockingQueue inputQueue,
                      ClientRegistry clientRegistry, BrokerStats stats,
                      Executor executor, Consumer<Connection> onClose) throws IOException {
        this.socket = socket;
        this.broker = broker;
        this.inputQueue = inputQueue;
        this.clientRegistry = clientRegistry;
        this.stats = stats;
        this.executor = executor;
        this.onClose = onClose;

        FrameReader reader;
        FrameWriter writer;
        try {
            reader = new FrameReader(new BufferedInputStream(socket.getInputStream()));
            writer = new FrameWriter(
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            throw e;
        }
        this.frameReader = reader;
        this.frameWriter = writer;
    }

    public void start() {
        executor.execute(new Reader(this));
    }

    /** false — clientId уже занят или соединение закрыто. */
    public boolean initClient(String clientId) {
        synchronized (this) {
            if (closed.get() || client != null) {
                return false;
            }
            if (!clientRegistry.tryRegister(clientId)) {
                return false;
            }

            IBlockingQueue outgoing = new ArrayBlockingMessageQueue(OUTGOING_QUEUE_CAPACITY);
            this.outgoingMessages = outgoing;
            this.client = new Client(clientId, outgoing);
            try {
                executor.execute(new Writer(this));
            } catch (RuntimeException e) {
                this.client = null;
                this.outgoingMessages = null;
                outgoing.close();
                clientRegistry.unregister(clientId);
                throw e;
            }
            return true;
        }
    }

    public void send(Frame frame) throws IOException {
        writeLock.lock();
        try {
            if (closed.get()) {
                throw new IOException("connection is closed");
            }
            frameWriter.writeFrame(frame);
        } finally {
            writeLock.unlock();
        }
    }

    public FrameReader getFrameReader() {
        return frameReader;
    }

    public Broker getBroker() {
        return broker;
    }

    public IBlockingQueue getInputQueue() {
        return inputQueue;
    }

    public Client getClient() {
        return client;
    }

    public BrokerStats getStats() {
        return stats;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        IBlockingQueue outgoing = outgoingMessages;
        if (outgoing != null) {
            outgoing.close();
        }

        Client current = client;
        if (current != null) {
            broker.unsubscribeAll(current);
            clientRegistry.unregister(current.getId());
        }

        try {
            socket.close();
        } catch (IOException ignored) {
        }

        if (onClose != null) {
            onClose.accept(this);
        }
    }
}
