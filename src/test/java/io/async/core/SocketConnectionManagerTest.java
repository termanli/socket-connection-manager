package io.async.core;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.junit.Assert.*;

/**
 * Created by j19li on 2017/11/28.
 */
public class SocketConnectionManagerTest {
    Thread testThread;

    @Test
    public void testConnectReadWriteSynchronize() throws Exception {
        startTestServerSocket(8088);
        SocketConnectionManager mgr = getSocketConnectionManager();
        Channel channel = mgr.connect("localhost", 8088);
        for (int i = 0; i < 10000; i++) {
            channel.write((i + "hello world!").getBytes());
            assertEquals('a', channel.read());
            byte[] arr = new byte["\nhello world\n".length()];
            assertEquals("\nhello world\n".length(), channel.read(arr, 0, arr.length));
            assertArrayEquals("\nhello world\n".getBytes(), Arrays.copyOfRange(arr, 0, arr.length));
        }
        assertEquals(-1, channel.read());

    }

    @Test
    public void testConnectReadWriteAsynchronous() throws Exception {
        testThread = Thread.currentThread();
        SocketConnectionManager mgr = getSocketConnectionManager();
        TaskCount count = new TaskCount();
        ThreadPoolExecutor serverPool = new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100000));
        serverPool.setThreadFactory(new DefaultThreadFactory("Server"));
        serverPool.setRejectedExecutionHandler((r, executor) -> System.out.println("server task rejected" + r));
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open(AsynchronousChannelGroup.withThreadPool(serverPool)).bind(new InetSocketAddress(8088), 10000);
        ServerTask task = new ServerTask();
        task.setServer(server);
        task.setTaskCount(0);
        LinkedBlockingDeque<Throwable> serverExceptions = new LinkedBlockingDeque<>();
        task.setServerExceptions(serverExceptions);
        server.accept(task, new CompletionHandler<AsynchronousSocketChannel, ServerTask>() {
            @Override
            public void completed(AsynchronousSocketChannel result, ServerTask attachment) {
                attachment.setCompletionHandler(this);
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                attachment.setReadBuffer(buffer);
                attachment.setServerChannel(result);
                result.read(buffer, attachment, new CompletionHandler<Integer, ServerTask>() {
                    @Override
                    public void completed(Integer result, ServerTask attachment) {
                        try {
//                            assertEquals("hello world!", new String(attachment.readBuffer.array(), 0, result));
                            int loopCount = Integer.parseInt(new String(attachment.readBuffer.array(), 0, result));
                            attachment.setLoopCount(loopCount);
                            ServerTask task = new ServerTask();
                            task.setServer(attachment.getServer());
                            task.setTaskCount(attachment.getTaskCount() + 1);
                            task.setServerExceptions(attachment.serverExceptions);
                            task.setCompletionHandler(attachment.completionHandler);
                            try {
                                attachment.getServer().accept(task, attachment.completionHandler);
                            } catch (Exception e) {
                                System.out.print("Invoke next accept failed");
                            }
                            attachment.write(0);
                        } catch (Throwable e) {
                            attachment.serverExceptions.offer(e);
                        }
                    }

                    @Override
                    public void failed(Throwable exc, ServerTask attachment) {
                        attachment.serverExceptions.offer(exc);
                    }
                });
            }

            @Override
            public void failed(Throwable exc, ServerTask attachment) {
                System.out.print("Accept failed");
            }
        });
        ArrayList<AsynchronousTestTask> tasks = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            if (mgr.getOpenChannelCount() != 50) {
                System.out.println("open channel count=" + mgr.getOpenChannelCount());
            }
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            AsynchronousTestTask testTask = new AsynchronousTestTask();
            tasks.add(testTask);
            testTask.setMgr(mgr);
            testTask.setTaskCount(count);
            testTask.setTaskId(i);
            testTask.run();
        }
        System.out.println("task publish complete");
        System.out.println("open channel count=" + mgr.getOpenChannelCount());
        int current_count = 0;
        while (count.taskCount < 10000) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            synchronized (count) {
                try {
                    count.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            if (count.taskCount > current_count) {
                current_count = count.taskCount;
                if (current_count % 100 == 0) {
                    System.out.println(current_count);
                    if (mgr.getOpenChannelCount() != 50) {
                        System.out.println("open channel count=" + mgr.getOpenChannelCount());
                    }
                }
            } else {
                if(mgr.getOpenChannelCount()==0){
                    break;
                }
            }
        }
        System.out.println("Test tasks finished:" + count.taskCount);
        server.close();
        boolean failed = false;
        for (Thread th : Thread.getAllStackTraces().keySet()) {
            if (th != Thread.currentThread()) {
                th.interrupt();
            }
        }
        System.out.println("tasks:" + tasks.size());
        for (AsynchronousTestTask t : tasks) {
            if (t.exceptions.size() > 0) {
                System.out.println("task-" + t.taskId + ":");
                System.out.println("-----------------");
                for (Throwable th : t.exceptions) {
                    th.printStackTrace();
                    failed = true;
                }
                System.out.println("-----------------");
            }

        }
        if(serverExceptions.size()>0){
            failed=true;
            System.out.println("Server Exceptions:");
            for(Throwable t:serverExceptions){
                t.printStackTrace();
            }
        }

        if (failed) {
            fail();
        }
    }

    private SocketConnectionManager getSocketConnectionManager() throws IOException {
        SocketConnectionManager mgr;
        ThreadPoolExecutor ioPool = new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(1000000));
        ioPool.setThreadFactory(new DefaultThreadFactory("IO"));
        ioPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.out.println("io task rejected" + r);
            }
        });
        ThreadPoolExecutor callBackPool = new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(1000000));
        callBackPool.setThreadFactory(new DefaultThreadFactory("CallBack"));
        callBackPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.out.println("callback task rejected" + r);
            }
        });
        mgr = new SocketConnectionManager(ioPool, callBackPool, new Properties() {{
            setProperty("READ_TIMEOUT", "10000");
            setProperty("CONCURRENT_CONNECTION_COUNT", "200");
        }});
        return mgr;
    }

    private void startTestServerSocket(int port) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                ServerSocket ss = null;
                try {
                    ss = new ServerSocket(port);
                    Socket socket = ss.accept();
                    InputStream is = socket.getInputStream();
                    OutputStream os = socket.getOutputStream();
                    for (int i = 0; i < 10000; i++) {
                        if (Thread.currentThread().isInterrupted()) {
                            break;
                        }
                        byte[] b = new byte[(i + "hello world!").length()];
                        is.read(b);
                        try {
                            assertArrayEquals((i + "hello world!").getBytes(), b);
                        } catch (Exception e) {
                            for (Thread th : Thread.getAllStackTraces().keySet()) {
                                th.interrupt();
                            }
                            throw e;
                        }
                        os.write('a');
                        os.flush();
                        os.write('\n');
                        os.flush();
                        os.write("hello world\n".getBytes());
                        os.flush();
                    }
                    os.close();
                    ss.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    private class ReadCallBack implements IntConsumer {
        int[] buffer;
        volatile boolean invoked = false;
        Object lock;
        private Channel channel;
        private int count;
        private AsynchronousTestTask testTask;

        @Override
        public void accept(int value) {
            buffer[0] = value;
            invoked = true;
            try {
                assertEquals('a', buffer[0]);
            } catch (Throwable e) {
                testTask.processAssertionErrors(e);
            }
            byte[] arr = new byte["\nhello world\n".length() + 2];
            channel.read(arr, 2, arr.length - 2, (int i) -> {
                synchronized (lock) {
                    lock.notifyAll();
                }
                try {
                    assertEquals("\nhello world\n".length(), i);
                } catch (Throwable e) {
                    testTask.processAssertionErrors(e);
                }
                try {
                    assertArrayEquals("Actual result is:" + new String(arr, 2, arr.length - 2) + ";", "\nhello world\n".getBytes(), Arrays.copyOfRange(arr, 2, arr.length));
                } catch (Throwable arrayComparisonFailure) {
                    testTask.processAssertionErrors(arrayComparisonFailure);
                }
                try {
                    testTask.readLoopBody(channel, ++count);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }, (Throwable ex) -> {
                testTask.processAssertionErrors(ex);
                channel.close();
            });
        }

        public void setBuffer(int[] buffer) {
            this.buffer = buffer;
        }

        public boolean isInvoked() {
            return invoked;
        }

        public void setLock(Object lock) {
            this.lock = lock;
        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public void setTestTask(AsynchronousTestTask testTask) {
            this.testTask = testTask;
        }
    }

    private class ServerTask {
        private AsynchronousServerSocketChannel server;
        private AsynchronousSocketChannel serverChannel;
        private int taskCount;
        private LinkedBlockingDeque<Throwable> serverExceptions;
        private ByteBuffer readBuffer;
        private CompletionHandler<AsynchronousSocketChannel, ServerTask> completionHandler;
        private int loopCount;

        private void write(Integer count) {
            if (count < loopCount) {
                serverChannel.write(ByteBuffer.wrap("a\nhello world\n".getBytes()), count, new CompletionHandler<Integer, Integer>() {
                    @Override
                    public void completed(Integer result, Integer attachment) {

                        write(attachment + 1);
                    }

                    @Override
                    public void failed(Throwable exc, Integer attachment) {

                        serverExceptions.offer(exc);
                    }
                });
            } else {
                try {
                    serverChannel.close();
                } catch (IOException e) {
                    serverExceptions.offer(e);
                }
            }
        }

        public void setServer(AsynchronousServerSocketChannel server) {
            this.server = server;
        }

        public AsynchronousServerSocketChannel getServer() {
            return server;
        }

        public void setServerChannel(AsynchronousSocketChannel serverChannel) {
            this.serverChannel = serverChannel;
        }

        public void setTaskCount(int count) {
            this.taskCount = count;
        }

        public int getTaskCount() {
            return taskCount;
        }

        public void setServerExceptions(LinkedBlockingDeque<Throwable> serverExceptions) {
            this.serverExceptions = serverExceptions;
        }

        public void setReadBuffer(ByteBuffer readBuffer) {
            this.readBuffer = readBuffer;
        }

        public void setCompletionHandler(CompletionHandler<AsynchronousSocketChannel,ServerTask> completionHandler) {
            this.completionHandler = completionHandler;
        }

        public void setLoopCount(int loopCount) {
            this.loopCount = loopCount;
        }
    }

    private class AsynchronousTestTask implements Runnable {
        Byte lock = 1;
        private SocketConnectionManager mgr;
        private TaskCount taskCount;
        private LinkedBlockingDeque<Throwable> exceptions = new LinkedBlockingDeque<>();
        private int taskId;
        private int retryTimes = 0;
        private int loopCount=new Random().nextInt(9000)+1000;
        void putException(Throwable ex) {
            exceptions.offer(ex);
        }

        public void setMgr(SocketConnectionManager mgr) {
            this.mgr = mgr;
        }


        @Override
        public void run() {
            try {
                mgr.connect("localhost", 8088, (Channel ch) -> {
                    ch.write(Integer.toString(loopCount).getBytes(), (Integer i) -> {
                        try {
                            assertEquals(Integer.toString(loopCount).length(), i.intValue());
                            readLoopBody(ch, 0);
                        } catch (Throwable e) {
                            processAssertionErrors(e);
                        }
                    }, (Throwable ex) -> {
                        processAssertionErrors(ex);
                    });
                }, (Throwable ex) -> {
                    if (retryTimes < 3) {
                        synchronized (this) {
                            retryTimes++;
                        }
                        run();
                    }
                });
            } catch (Exception e) {
                processAssertionErrors(e);
            }
        }


        void readLoopBody(Channel channel, int count) throws InterruptedException {
            if (count < loopCount) {
                final int[] b = {-1};
                ReadCallBack callBack = new ReadCallBack();
                callBack.setLock(lock);
                callBack.setBuffer(b);
                callBack.setChannel(channel);
                callBack.setCount(count);
                callBack.setTestTask(this);
                channel.read(callBack, (Throwable ex) -> {
                    processAssertionErrors(ex);
                });
            } else {
                channel.read((int i) -> {
                    taskCount.inc();
                    channel.close();
                    try {
                        assertTrue("expected -1 but get " + i, i < 0);
                    } catch (Throwable e) {
                        processAssertionErrors(e);
                    }
                }, (Throwable ex) -> {
                    processAssertionErrors(ex);
                    channel.close();
                });
            }

        }

        public void setTaskCount(TaskCount taskCount) {
            this.taskCount = taskCount;
        }

        private void processAssertionErrors(Throwable e) {
            putException(e);
            /*for (Thread th : Thread.getAllStackTraces().keySet()) {
                th.interrupt();
            }*/
        }

        public void setTaskId(int taskId) {
            this.taskId = taskId;
        }
    }

    private class TaskCount {
        volatile int taskCount = 0;

        void inc() {
            synchronized (this) {
                taskCount++;
                this.notifyAll();
            }
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String namePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            this.namePrefix = namePrefix + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}