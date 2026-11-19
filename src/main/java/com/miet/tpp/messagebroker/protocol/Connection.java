package com.miet.tpp.messagebroker.protocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.miet.tpp.messagebroker.brokerQueue.ArrayBlockingMessageQueue;
import com.miet.tpp.messagebroker.brokerQueue.IBlockingQueue;
import com.miet.tpp.messagebroker.codec.Frame;
import com.miet.tpp.messagebroker.codec.FrameReader;
import com.miet.tpp.messagebroker.codec.FrameWriter;
import com.miet.tpp.messagebroker.codec.Reader;
import com.miet.tpp.messagebroker.codec.Writer;
import com.miet.tpp.messagebroker.messages.ReentrantRWBroker;

/**
 * Connection
 */
public class Connection {
    private static final int OUTGOING_QUEUE_CAPACITY = 1024;

    private final Socket socket;
    private final FrameReader frameReader;
    private final FrameWriter frameWriter;
    private final ReentrantRWBroker broker;
    private final IBlockingQueue inputQueue;
    private final Consumer<Connection> onClose;
    private final Object writeLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Client client;
    private volatile IBlockingQueue outgoingMessages;
    private Thread readerThread;
    private Thread writerThread;

    public Connection(Socket socket, ReentrantRWBroker broker, IBlockingQueue inputQueue)
            throws IOException {
        this(socket, broker, inputQueue, null);
    }

    public Connection(Socket socket, ReentrantRWBroker broker, IBlockingQueue inputQueue,
                      Consumer<Connection> onClose) throws IOException {
        this.socket = socket;
        this.broker = broker;
        this.inputQueue = inputQueue;
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
        if (readerThread != null) {
            throw new IllegalStateException("connection already started");
        }
        readerThread = new Thread(new Reader(this),
                "reader-" + socket.getRemoteSocketAddress());
        readerThread.start();
    }

    public void initClient(String clientId) {
        synchronized (this) {
            if (closed.get()) {
                throw new IllegalStateException("connection is closed");
            }
            if (client != null) {
                throw new IllegalStateException("client already connected");
            }

            IBlockingQueue outgoing = new ArrayBlockingMessageQueue(OUTGOING_QUEUE_CAPACITY);
            this.outgoingMessages = outgoing;
            this.client = new Client(clientId, outgoing);
            this.writerThread = new Thread(new Writer(this), "writer-" + clientId);
            this.writerThread.start();
        }
    }

    public void send(Frame frame) throws IOException {
        synchronized (writeLock) {
            if (closed.get()) {
                throw new IOException("connection is closed");
            }
            frameWriter.writeFrame(frame);
        }
    }

    public FrameReader getFrameReader() {
        return frameReader;
    }

    public ReentrantRWBroker getBroker() {
        return broker;
    }

    public IBlockingQueue getInputQueue() {
        return inputQueue;
    }

    public Client getClient() {
        return client;
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
        }

        try {
            socket.close();
        } catch (IOException ignored) {
        }

        Thread rt = readerThread;
        if (rt != null) {
            rt.interrupt();
        }
        Thread wt = writerThread;
        if (wt != null) {
            wt.interrupt();
        }

        if (onClose != null) {
            onClose.accept(this);
        }
    }
}
