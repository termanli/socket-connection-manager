package io.async.core;

import org.javatuples.Pair;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Created by j19li on 2017/11/27.
 */
public class SocketConnectionManager implements Runnable {
    private static final Properties defaultConfig = new Properties() {{
        put("CONNECTION_TIMEOUT", Long.toString(1000));
        put("READ_TIMEOUT", Long.toString(1000));
        put("READ_BUFFER_CAPACITY", Integer.toString(1024));
        put("READ_QUEUE_SIZE", Integer.toString(100));
        put("READ_TASK_QUEUE_SIZE", Integer.toString(1024));
        put("CONCURRENT_CONNECTION_COUNT", Integer.toString(100));
        put("CONNECTION_QUEUE_SIZE", Integer.toString(1000000));
    }};
    private final AsynchronousChannelGroup channelGroup;
    private final ExecutorService callBackThreadPool;
    private final ExecutorService ioThreadPool;
    private int connectionQueueSize;
    private Map<Channel, AsynchronousSocketChannelWrapper> channelWrapperMapping = new ConcurrentHashMap<>();
    private LinkedBlockingDeque<Pair<BiConsumer<Channel, AsynchronousSocketChannelWrapper>,Consumer<Throwable>>> connectionQueue;
    /*private LinkedBlockingDeque<Channel> channels = new LinkedBlockingDeque();*/
    private long connectTimeout;
    private long readTimeOut;
    private int readBufferCapacity;
    private int readQueueSize;
    private int readTaskQueueSize;
    private int concurrentConnectionCount;
    private Thread thread;
    private int writeTimeOut;

    public SocketConnectionManager(ExecutorService ioThreadPool, ExecutorService callBackThreadPool) throws IOException {
        this(ioThreadPool, callBackThreadPool, new Properties());
    }

    public SocketConnectionManager(ExecutorService ioThreadPool, ExecutorService callBackThreadPool, Properties config) throws IOException {
        this.ioThreadPool = ioThreadPool;
        this.channelGroup = AsynchronousChannelGroup.withThreadPool(ioThreadPool);
        this.callBackThreadPool = callBackThreadPool;
        Properties props = new Properties();
        props.putAll(defaultConfig);
        props.putAll(config);
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            switch (key) {
                case "CONNECTION_TIMEOUT":
                    this.connectTimeout = Long.parseLong(value);
                    break;
                case "READ_TIMEOUT":
                    this.readTimeOut = Long.parseLong(value);
                    break;
                case "READ_BUFFER_CAPACITY":
                    this.readBufferCapacity = Integer.parseInt(value);
                    break;
                case "READ_QUEUE_SIZE":
                    this.readQueueSize = Integer.parseInt(value);
                    break;
                case "READ_TASK_QUEUE_SIZE":
                    this.readTaskQueueSize = Integer.parseInt(value);
                    break;
                case "CONCURRENT_CONNECTION_COUNT":
                    this.concurrentConnectionCount = Integer.parseInt(value);
                case "CONNECTION_QUEUE_SIZE":
                    this.connectionQueueSize=Integer.parseInt(value);
            }
        }
        this.connectionQueue= new LinkedBlockingDeque<>(this.connectionQueueSize);
    }

    public void start() {
        if (thread == null) {
            thread = new Thread(this);
            thread.start();
        }
    }

    public void stop() {
        if (thread != null) {
            thread.interrupt();
        }
    }

    public long getReadTimeOut() {
        return readTimeOut;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public int getReadBufferCapacity() {
        return readBufferCapacity;
    }

    public int getReadQueueSize() {
        return readQueueSize;
    }

    public int getReadTaskQueueSize() {
        return readTaskQueueSize;
    }

    public Channel connect(String host, int port) {
        try {
            Channel channel = new Channel(this);
            AsynchronousSocketChannel ch = AsynchronousSocketChannel.open();
            AsynchronousSocketChannelWrapper wrapper = new AsynchronousSocketChannelWrapper(ch);
            putWrapper(channel, wrapper);
            doConnect(host, port, channel, ch);
            if (wrapper.isConnectComplete()) {
                if (!wrapper.isConnectSuccess()) {
                    if (wrapper.getConnectException() != null) {
                        throw new AsyncIOException("Connection Failed", wrapper.getConnectException());
                    } else {
                        throw new AsyncIOException("Connection Failed");
                    }
                }
            } else {
                throw new AsyncIOException("Connection Timeout");
            }
            return channel;
        } catch (IOException e) {
            throw new AsyncIOException("Failed open channel", e);
        }
    }

    private void putWrapper(Channel channel, AsynchronousSocketChannelWrapper wrapper) {
        /*HashMap<Channel, AsynchronousSocketChannelWrapper> m=new HashMap<>(channelWrapperMapping);
        m.put(channel, wrapper);
        channelWrapperMapping=m;*/
        channelWrapperMapping.put(channel, wrapper);

    }

    public void connect(String host, int port, Consumer<Channel> onSuccess, Consumer<Throwable> onFail) {
        try {
            if (channelWrapperMapping.size() >= concurrentConnectionCount) {
                BiConsumer<Channel, AsynchronousSocketChannelWrapper> s=(Channel ch,AsynchronousSocketChannelWrapper wrapper)->{
                    wrapper.setOnConnectFailConsumer(onFail);
                    wrapper.setOnConnectSuccessConsumer(onSuccess);
                    putWrapper(ch, wrapper);
                    doConnect(host, port, ch, wrapper.getChannel());
                };
                Consumer<Throwable> f=(Throwable e)->{
                    onFail.accept(e);
                };
                if(!connectionQueue.offer(new Pair<>(s,f))){
                    throw new AsyncIOException("connection queue is full");
                }
            } else {
                Channel channel = new Channel(this);
                AsynchronousSocketChannel ch = AsynchronousSocketChannel.open();
                AsynchronousSocketChannelWrapper wrapper = new AsynchronousSocketChannelWrapper(ch);
                wrapper.setOnConnectFailConsumer(onFail);
                wrapper.setOnConnectSuccessConsumer(onSuccess);
                putWrapper(channel, wrapper);
                doConnect(host, port, channel, ch);
            }
        } catch (IOException e) {
            throw new AsyncIOException("Failed open channel", e);
        }
    }

    private void doConnect(String host, int port, Channel channel, AsynchronousSocketChannel ch) {
        try {
            AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
            if (wrapper != null) {
                wrapper.setConnectStartTime(System.currentTimeMillis());
            }
            ch.connect(new InetSocketAddress(host, port), channel, new CompletionHandler<Void, Channel>() {
                @Override
                public void completed(Void result, Channel attachment) {

                    AsynchronousSocketChannelWrapper wrapper = getWrapper(attachment);
                    if (wrapper != null) {
                        /*channels.offer(attachment);*/
                        wrapper.connectSuccess();
                        if (wrapper.getOnConnectSuccessConsumer() != null) {
                            wrapper.getOnConnectSuccessConsumer().accept(attachment);
                        }
                        wrapper.read(attachment);
                    }
                }

                @Override
                public void failed(Throwable exc, Channel attachment) {
                    AsynchronousSocketChannelWrapper wrapper = getWrapper(attachment);
                    if (wrapper != null) {
                        wrapper.connectFailed(exc);
                        if (wrapper.getOnConnectFailConsumer() != null) {
                            wrapper.getOnConnectFailConsumer().accept(exc);
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new AsyncIOException("Connect Failed", e);
        }
    }

    void disconnect(Channel channel) {
        AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
        if (wrapper != null) {
            try {
                wrapper.getChannel().close();
            } catch (IOException e) {
                throw new AsyncIOException("Failed to close channel", e);
            } finally {
                /*HashMap<Channel, AsynchronousSocketChannelWrapper> m=new HashMap<>(channelWrapperMapping);
                m.remove(channel);
                channelWrapperMapping=m;*/
                channelWrapperMapping.remove(channel);
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            while (channelWrapperMapping.size()<concurrentConnectionCount){
                Pair<BiConsumer<Channel, AsynchronousSocketChannelWrapper>,Consumer<Throwable>> task=connectionQueue.poll();
                if(task==null){
                    break;
                }else{
                    Channel channel = new Channel(this);
                    try {
                        AsynchronousSocketChannel ch=AsynchronousSocketChannel.open();
                        AsynchronousSocketChannelWrapper wrapper = new AsynchronousSocketChannelWrapper(ch);
                        task.getValue0().accept(channel,wrapper);
                    } catch (IOException e) {
                        task.getValue1().accept(e);
                    }
                }
            }
            for (Channel channel : channelWrapperMapping.keySet()) {
                if (channel != null) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
                    if (wrapper != null) {
                        ReadTask task = new ReadTask();
                        task.setChannel(channel);
                        if (wrapper.isConnectSuccess()) {
                            callBackThreadPool.submit(task);
                        } else {
                            if (System.currentTimeMillis() - wrapper.getConnectStartTime() > connectTimeout) {
                                if (wrapper.getOnConnectFailConsumer() != null) {
                                    disconnect(channel);
                                    wrapper.getOnConnectFailConsumer().accept(new AsyncIOException("Connection Timeout"));
                                }
                            }
                        }
                    }
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private AsynchronousSocketChannelWrapper getWrapper(Channel channel) {
        /*synchronized (channelWrapperMapping) {*/
        return channelWrapperMapping.get(channel);
        /*}*/
    }

    public ReadGuarder read(Channel channel, byte[] result, int off, int length) {
        AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
        return wrapper.commitRead(result, off, length);
    }

    public ReadGuarder read(Channel channel, byte[] result, int off, int length, IntConsumer onSuccess, Consumer<Throwable> onFail) {
        AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
        return wrapper.commitRead(result, off, length, onSuccess, onFail);
    }

    public WriteGuarder write(Channel channel, byte[] buffer, int off, int length) {
        AsynchronousSocketChannelWrapper wrapper=getWrapper(channel);
        WriteGuarder guarder=new WriteGuarder();
        wrapper.getChannel().write(ByteBuffer.wrap(buffer),getWriteTimeOut(),TimeUnit.MILLISECONDS,channel,guarder);
        return guarder;
    }

    public int getWriteTimeOut() {
        return writeTimeOut;
    }

    public void write(Channel channel, byte[] buffer, int off, int length, Consumer<Integer> onComplete, Consumer<Throwable> onFailure) {
        AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
        WriteGuarder guarder = new WriteGuarder();
        guarder.setOnSuccess(onComplete);
        guarder.setOnFailed(onFailure);
        wrapper.getChannel().write(ByteBuffer.wrap(buffer), getWriteTimeOut(), TimeUnit.MILLISECONDS, channel, guarder);
    }


    private class ReadCompletionHandler implements CompletionHandler<Integer, Channel> {

        @Override
        public void completed(Integer result, Channel attachment) {
            AsynchronousSocketChannelWrapper wrapper = getWrapper(attachment);
            if (wrapper != null) {
                if (result >= 0) {
                    if (result > 0) {
                        wrapper.refreshBuffer();
                    }
                    wrapper.read(attachment);
                } else {
                    wrapper.setReadEOF();
                }
            }
        }

        @Override
        public void failed(Throwable exc, Channel attachment) {
            AsynchronousSocketChannelWrapper wrapper = getWrapper(attachment);
            if (wrapper != null) {
                wrapper.readFail(exc);
            }
        }
    }

    //private class WriteCompletionHandler implements CompletionHandler{}
    private class AsynchronousSocketChannelWrapper {
        AsynchronousSocketChannel channel;
        final Byte connectLock = 0;
        private volatile boolean connectComplete;
        private volatile boolean connectSuccess;
        private volatile Throwable connectException;
        private Consumer<Channel> onConnectSuccessConsumer;
        private Consumer<Throwable> onConnectFailConsumer;
        private boolean readEOF = false;
        private ByteBuffer buffer = ByteBuffer.allocate(readBufferCapacity);
        private ReadCompletionHandler readCompletionHandler = new ReadCompletionHandler();
        private volatile boolean readFailed = false;
        private volatile Throwable readException;
        private LinkedBlockingDeque<ByteBuffer> readQueue = new LinkedBlockingDeque(readQueueSize);
        private volatile ByteBuffer tmpBuff;
        private volatile boolean readBreakOnQueueFull = false;
        private volatile long readCount = 0;
        private LinkedBlockingDeque<ReadGuarder> readTaskQueue = new LinkedBlockingDeque(readTaskQueueSize);
        private volatile ByteBuffer currentReadBuffer;
        private ReadGuarder currentReadTask;
        private volatile long connectStartTime;
        private long writeCount=0;

        AsynchronousSocketChannelWrapper(AsynchronousSocketChannel channel) {
            this.channel = channel;
        }

        void connectFailed(Throwable ex) {
            connectComplete = true;
            connectSuccess = false;
            connectException = ex;
            synchronized (connectLock) {
                connectLock.notifyAll();
            }
        }

        void connectSuccess() {
            connectComplete = true;
            connectSuccess = true;
            synchronized (connectLock) {
                connectLock.notifyAll();
            }
        }

        boolean isConnectSuccess() {
            return connectSuccess;
        }

        Throwable getConnectException() {
            return connectException;
        }

        public void setConnectException(Throwable connectException) {
            this.connectException = connectException;
        }

        Consumer<Channel> getOnConnectSuccessConsumer() {
            return onConnectSuccessConsumer;
        }

        void setOnConnectSuccessConsumer(Consumer<Channel> onConnectSuccessConsumer) {
            this.onConnectSuccessConsumer = onConnectSuccessConsumer;
        }

        Consumer<Throwable> getOnConnectFailConsumer() {
            return onConnectFailConsumer;
        }

        void setOnConnectFailConsumer(Consumer<Throwable> onConnectFailConsumer) {
            this.onConnectFailConsumer = onConnectFailConsumer;
        }

        boolean isConnectComplete() {
            try {
                synchronized (connectLock) {
                    connectLock.wait(connectTimeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return this.connectComplete;
        }

        AsynchronousSocketChannel getChannel() {
            return channel;
        }

        void read(Channel ch) {
            if (!readFailed) {
                if (!isReadEOF() && !readBreakOnQueueFull) {
                    synchronized (buffer) {
                        channel.read(buffer, readTimeOut, TimeUnit.MILLISECONDS, ch, readCompletionHandler);
                    }
                }
            }
        }

        boolean refreshBuffer() {
            if (readQueue.remainingCapacity() > 0) {
                byte[] ba = null;
                synchronized (buffer) {
                    ba = new byte[buffer.position()];
                    System.arraycopy(buffer.array(), 0, ba, 0, ba.length);
                    buffer.position(0);
                }
                tmpBuff = ByteBuffer.wrap(ba);
                boolean rs = readQueue.offer(ByteBuffer.wrap(ba));
                if (!rs) {
                    readBreakOnQueueFull = true;
                } else {
                    readBreakOnQueueFull = false;
                    tmpBuff = null;
                    updateReadCount(ba.length);
                }
                return rs;
            } else {
                readBreakOnQueueFull = true;
                return false;
            }
        }

        void updateReadCount(Integer result) {
            synchronized (this) {
                readCount += result;
            }
        }

        long getReadCount() {
            return readCount;
        }

        boolean isReadEOF() {
            return readEOF;
        }

        void setReadEOF() {
            this.readEOF = true;
        }

        ReadGuarder commitRead(byte[] result, int off, int length) {
            ReadGuarder guarder = new ReadGuarder(result, off, length);
            if (!readTaskQueue.offer(guarder)) {
                throw new AsyncIOException("Read task queue full");
            }
            return guarder;
        }

        ReadGuarder commitRead(byte[] result, int off, int length, IntConsumer onSuccess, Consumer<Throwable> onFail) {
            ReadGuarder guarder = new ReadGuarder(result, off, length);
            guarder.setOnSuccess(onSuccess);
            guarder.setOnFail(onFail);
            if (!readTaskQueue.offer(guarder)) {
                throw new AsyncIOException("Read task queue full");
            }
            return guarder;
        }

        void runReadTask(Channel ch) {
            if (currentReadTask == null) {
                currentReadTask = readTaskQueue.poll();
            }
            if (currentReadTask != null) {
                while (currentReadTask.remaining() > 0) {
                    if (this.currentReadBuffer == null || (!this.currentReadBuffer.hasRemaining())) {
                        this.currentReadBuffer = readQueue.poll();
                    }
                    if (this.currentReadBuffer == null) {
                        if (isReadEOF()) {
                            if (currentReadTask.getReadCount() <= 0) {
                                currentReadTask.setReadCount(-1);
                            }
                            currentReadTask.finish();
                        }
                        if (readFailed) {
                            currentReadTask.finish(readException);
                        }
                        break;
                    }
                    int _l = Math.min(currentReadTask.remaining(), this.currentReadBuffer.remaining());
                    byte[] _b = new byte[_l];
                    this.currentReadBuffer.get(_b, 0, _l);
                    currentReadTask.offer(_b, 0, _l);
                }
                if (currentReadTask.isFinished()) {
                    currentReadTask = readTaskQueue.poll();
                }
            }

            if (this.readBreakOnQueueFull) {
                if (this.refreshBuffer() && (!this.readFailed)) {
                    this.read(ch);
                }
            }
        }

        public boolean isReadFailed() {
            return readFailed;
        }

        public Throwable getReadException() {
            return readException;
        }

        public void readFail(Throwable exc) {
            this.readFailed = true;
            this.readException = exc;
        }

        public long getConnectStartTime() {
            return connectStartTime;
        }

        public void setConnectStartTime(long connectStartTime) {
            this.connectStartTime = connectStartTime;
        }

        public void updateWriteCount(Integer result) {
            synchronized (this) {
                writeCount += result;
            }
        }

        public long getWriteCount() {
            return writeCount;
        }
    }

    class ReadGuarder {

        private final byte[] dest;
        private final int off;
        private final int length;
        private volatile boolean finished = false;
        private volatile int position;
        private volatile int readCount = 0;
        private volatile boolean readFailed;
        private volatile Throwable readException;
        private IntConsumer onSuccess;
        private Consumer<Throwable> onFail;

        ReadGuarder(byte[] dest, int off, int length) {
            this.dest = dest;
            this.off = off;
            this.length = length;
            position = off;
        }

        void offer(byte[] src, int off, int len) {
            synchronized (this) {
                System.arraycopy(src, off, dest, position, len);
                position += len;
                if (position == length+this.off) {
                    finished = true;
                }
                readCount += len;
            }
            onFinished();
        }

        private void onFinished() {
            if (finished) {
                if (readFailed) {
                    if (this.onFail != null) {
                        callBackThreadPool.submit(() -> {
                            this.onFail.accept(this.readException);
                            synchronized (this) {
                                this.notifyAll();
                            }
                        });
                    } else {
                        synchronized (this) {
                            this.notifyAll();
                        }
                    }
                } else {
                    if (this.onSuccess != null) {
                        CallBackTask task = new CallBackTask();
                        task.setTaskType(TaskType.success);
                        task.setGuarder(this);
                        task.setReadCount(readCount);
                        callBackThreadPool.submit(task);
                    } else {
                        synchronized (this) {
                            this.notifyAll();
                        }
                    }
                }
            }
        }

        public boolean isFinished(long timeout) {
            if (!finished) {
                try {
                    synchronized (this) {
                        this.wait(timeout);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    return finished;
                }
            }else{
                return finished;
            }
        }

        public int remaining() {
            return length + off - position;
        }

        boolean isFinished() {
            return finished;
        }

        public int getReadCount() {
            return readCount;
        }

        void setReadCount(int readCount) {
            this.readCount = readCount;
        }

        void finish() {
            finished = true;
            onFinished();
        }

        public void finish(Throwable readException) {
            this.readFailed = true;
            this.readException = readException;
            finish();
        }

        public boolean isReadFailed() {
            return readFailed;
        }

        public Throwable getReadException() {
            return readException;
        }

        void setOnSuccess(IntConsumer onSuccess) {
            this.onSuccess = onSuccess;
        }

        void setOnFail(Consumer<Throwable> onFail) {
            this.onFail = onFail;
        }
    }

    private class ReadTask implements Runnable {

        private Channel channel;

        @Override
        public void run() {
            AsynchronousSocketChannelWrapper wrapper = getWrapper(channel);
            synchronized (channel) {
                wrapper.runReadTask(channel);
            }
        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }
    }

    private class CallBackTask implements Runnable {

        private TaskType taskType;
        private ReadGuarder guarder;
        private int readCount;

        @Override
        public void run() {
            switch (taskType) {
                case success:
                    guarder.onSuccess.accept(readCount);
                    synchronized (guarder) {
                        guarder.notifyAll();
                    }
                    break;
            }
        }

        public void setTaskType(TaskType taskType) {
            this.taskType = taskType;
        }

        public void setGuarder(ReadGuarder guarder) {
            this.guarder = guarder;
        }

        public void setReadCount(int readCount) {
            this.readCount = readCount;
        }
    }

    private enum TaskType {
        success(0),
        fail(1);

        private int value;

        TaskType(int value) {

            this.value = value;
        }
    }

    public class WriteGuarder implements CompletionHandler<Integer, Channel> {
        private volatile boolean writeFailed=false;
        private volatile Throwable writeException;
        private volatile boolean finished=false;
        private Consumer<Integer> onSuccess;
        private Consumer<Throwable> onFailed;


        public boolean isFinished(int timeout) {
            if (!finished) {
                try {
                    synchronized (this) {
                        this.wait(timeout);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    return finished;
                }
            }else{
                return finished;
            }
        }
        public boolean isFinished(){
            return finished;
        }
        public boolean isWriteFailed() {
            return writeFailed;
        }

        public Throwable getWriteException() {
            return writeException;
        }

        @Override
        public void completed(Integer result, Channel attachment) {
            AsynchronousSocketChannelWrapper wrapper=channelWrapperMapping.get(attachment);
            wrapper.updateWriteCount(result);
            writeFailed=false;
            finished=true;
            synchronized (this) {
                this.notifyAll();
            }
            if(this.onSuccess!=null){
                this.onSuccess.accept(result);
            }
        }

        @Override
        public void failed(Throwable exc, Channel attachment) {
            writeFailed=true;
            finished=true;
            writeException=exc;
            synchronized (this) {
                this.notifyAll();
            }
            if(this.onFailed!=null){
               this.onFailed.accept(exc);
            }
        }

        public void setOnSuccess(Consumer<Integer> onSuccess) {
            this.onSuccess = onSuccess;
        }

        public void setOnFailed(Consumer<Throwable> onFailed) {
            this.onFailed = onFailed;
        }
    }
}
