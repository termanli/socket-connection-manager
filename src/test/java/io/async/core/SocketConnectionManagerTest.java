package io.async.core;

import io.async.core.Channel;
import io.async.core.SocketConnectionManager;
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
        SocketConnectionManager mgr =  getSocketConnectionManager();
        mgr.start();
        Channel channel = mgr.connect("localhost", 8088);
        for (int i = 0; i < 10000; i++) {
            channel.write((i+"hello world!").getBytes());
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
        mgr.start();
        TaskCount count = new TaskCount();
        ThreadPoolExecutor serverPool = new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100000));

        serverPool.setThreadFactory(new DefaultThreadFactory("Server"));
        serverPool.setRejectedExecutionHandler((r, executor) -> System.out.println("server task rejected" + r));
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open(AsynchronousChannelGroup.withThreadPool(serverPool)).bind(new InetSocketAddress(8088), 10000);
        ServerTask task = new ServerTask();
        task.setServer(server);
        task.setTaskCount(0);

        server.accept(task, new CompletionHandler<AsynchronousSocketChannel, ServerTask>() {
            @Override
            public void completed(AsynchronousSocketChannel result, ServerTask attachment) {
                ByteBuffer buffer=ByteBuffer.allocate(1024);
                Future<Integer> rs=result.read(buffer);
                try {
                    int cnt=rs.get(100,TimeUnit.MILLISECONDS);
                    assertEquals("hello world!",new String(buffer.array(),0,cnt));
                }  catch (Throwable e){
                    for(Thread t:Thread.getAllStackTraces().keySet()){
                        t.interrupt();
                    }
                    try {
                        throw e;
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    } catch (ExecutionException e1) {
                        e1.printStackTrace();
                    } catch (TimeoutException e1) {
                        e1.printStackTrace();
                    }
                }
                ServerTask task = new ServerTask();
                task.setServer(attachment.getServer());
                task.setTaskCount(attachment.getTaskCount() + 1);
                try {
                    attachment.getServer().accept(task, this);
                } catch (Exception e) {
                    System.out.print("Invoke next accept failed");
                }
                attachment.setServerChannel(result);
                attachment.write(0);
            }

            @Override
            public void failed(Throwable exc, ServerTask attachment) {
                System.out.print("Accept failed");
            }
        });
        ArrayList<AsynchronousTestTask> tasks = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            AsynchronousTestTask testTask = new AsynchronousTestTask();
            tasks.add(testTask);
            testTask.setMgr(mgr);
            testTask.setTaskCount(count);
            testTask.setTaskId(i);
            testTask.run();
            Thread.sleep(10);
        }
        System.out.println("task publish complete");
        int current_count = 0;
        while (count.taskCount < 1000) {
            if(Thread.currentThread().isInterrupted()){
                break;
            }
            synchronized (count) {
                if (count.taskCount > current_count) {
                    current_count = count.taskCount;
                    System.out.println(current_count);
                }
                try {
                    count.wait(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
        server.close();
        boolean failed=false;
        for (AsynchronousTestTask t : tasks) {
            for (Throwable th : t.exceptions) {
                th.printStackTrace();
                failed=true;
            }
        }
        if(failed){
            fail();
        }
    }

    private SocketConnectionManager getSocketConnectionManager() throws IOException {
        SocketConnectionManager mgr;
        ThreadPoolExecutor ioPool=new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(1000000));
        ioPool.setThreadFactory(new DefaultThreadFactory("IO"));
        ioPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.out.println("io task rejected"+r);
            }
        });
        ThreadPoolExecutor callBackPool=new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(1000000));
        callBackPool.setThreadFactory(new DefaultThreadFactory("CallBack"));
        callBackPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.out.println("callback task rejected"+r);
            }
        });
        mgr = new SocketConnectionManager(ioPool, callBackPool,new Properties(){{
            setProperty("READ_TIMEOUT","10000");
            setProperty("CONCURRENT_CONNECTION_COUNT","100");
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
                    InputStream is=socket.getInputStream();
                    OutputStream os = socket.getOutputStream();
                    for (int i = 0; i < 10000; i++) {
                        if(Thread.currentThread().isInterrupted()){
                            break;
                        }
                        byte[] b=new byte[(i+"hello world!").length()];
                        is.read(b);
                        try {
                            assertArrayEquals((i+"hello world!").getBytes(),b);
                        } catch (Exception e) {
                            for(Thread th:Thread.getAllStackTraces().keySet()){
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
                e.printStackTrace();
                testTask.processAssertionErrors(e);
            }
            byte[] arr = new byte["\nhello world\n".length()];
            channel.read(arr, 0, arr.length, (int i) -> {
                synchronized (lock) {
                    lock.notifyAll();
                }
                try {
                    assertEquals("\nhello world\n".length(), i);
                } catch (Throwable e) {
                    e.printStackTrace();
                    testTask.processAssertionErrors(e);
                }
                try {
                    assertArrayEquals("Actual result is:" + new String(arr) + ";", "\nhello world\n".getBytes(), Arrays.copyOfRange(arr, 0, arr.length));
                } catch (Throwable arrayComparisonFailure) {
                    arrayComparisonFailure.printStackTrace();
                    testTask.processAssertionErrors(arrayComparisonFailure);
                }
                try {
                    testTask.readLoopBody(channel,++count);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }, (Throwable ex) -> {
                ex.printStackTrace();
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
    private class ServerTask{
        private AsynchronousServerSocketChannel server;
        private AsynchronousSocketChannel serverChannel;
        private int taskCount;

        private void write(Integer count) {
            if (count<1000) {
                serverChannel.write(ByteBuffer.wrap("a\nhello world\n".getBytes()),count, new CompletionHandler<Integer, Integer>() {
                    @Override
                    public void completed(Integer result, Integer attachment) {
                        write(attachment+1);
                    }

                    @Override
                    public void failed(Throwable exc, Integer attachment) {
                        exc.printStackTrace();
                    }
                });
            }
            else{
                try {
                    serverChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
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
    }
    private class AsynchronousTestTask implements Runnable {
        Byte lock = 1;
        private SocketConnectionManager mgr;
        private TaskCount taskCount;
        private LinkedBlockingDeque<Throwable> exceptions=new LinkedBlockingDeque<>();
        private int taskId;
        private int retryTimes=0;
        void putException(Throwable ex){
            exceptions.offer(ex);
        }
        public void setMgr(SocketConnectionManager mgr) {
            this.mgr = mgr;
        }


        @Override
        public void run() {
            try {
                mgr.connect("localhost", 8088, (Channel ch) -> {
                    try {
                        ch.write("hello world!".getBytes());
                        readLoopBody(ch, 0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }, (Throwable ex) -> {
                    ex.printStackTrace();
                    if(retryTimes<3) {
                        synchronized (this){
                            retryTimes++;
                        }
                        run();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }



        void readLoopBody(Channel channel, int count) throws InterruptedException {
            if (count < 1000) {
                final int[] b = {-1};
                ReadCallBack callBack = new ReadCallBack();
                callBack.setLock(lock);
                callBack.setBuffer(b);
                callBack.setChannel(channel);
                callBack.setCount(count);
                callBack.setTestTask(this);
                channel.read(callBack, (Throwable ex) -> {
                    ex.printStackTrace();
                    channel.close();
                });
            }else{
                channel.read((int i) -> {
                    taskCount.inc();
                    channel.close();
                    try {
                        assertTrue("expected -1 but get "+i,i < 0);
                    } catch (Throwable e) {
                        e.printStackTrace();
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
            e.printStackTrace();
            putException(e);
            for (Thread th : Thread.getAllStackTraces().keySet()) {
                th.interrupt();
            }
        }

        public void setTaskId(int taskId) {
            this.taskId = taskId;
        }
    }
    private class TaskCount{
        volatile int taskCount=0;
        void inc(){
            synchronized (this){
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
            this.namePrefix = namePrefix+"-thread-";
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