# socket-connection-manager
Connect socket using java nio2 api

##Getting Start
```java
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
         /**
         *To initialize SocketConnectionManager need two thread pool, one for background io process. 
         *And another for the callback operations when io access complete.
         *A propperty could inject configurations to SocketConnectionManager
         **/
         SocketConnectionManager mgr = new SocketConnectionManager(ioPool, callBackPool,new Properties(){{
             setProperty("READ_TIMEOUT","10000");
             setProperty("CONCURRENT_CONNECTION_COUNT","300");
         }});
         /**
         *Start the background thread to process requests
         **/
         mgr.start();
          mgr.connect("localhost", 8088, (Channel ch) -> {
                channel.read((Int i)->{
                    // process readed byte
                }, (Throwable ex) -> {
                    // process read failure
                    channel.close();
                });
              }, 
              (Throwable ex) -> {
              //connect failed callback
              });
```
          
##Available configurations and default values,

Property Key | Description | Default Value
-|- |- 
CONNECTION_TIMEOUT | Timeout value for connect operation in miniseconds | 1000
READ_TIMEOUT|Timeout value for read operation in miniseconds|1000
READ_BUFFER_CAPACITY|Size in byte of one buffer for read per channel|1024
READ_QUEUE_SIZE|Max buffer in queue when the read operation will stop when queue full|100
READ_TASK_QUEUE_SIZE|Read request from caller will be queued per channel, this is the max size of the queue|1024
CONCURRENT_CONNECTION_COUNT|Max connections open at same time|100
CONNECTION_QUEUE_SIZE|Connect request from caller will be queued when the concurrent limitaion reached. This is the max size of the queue|1000000



 
 