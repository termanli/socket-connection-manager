# socket-connection-manager
Connect socket using java nio2 api
![alternate text](https://travis-ci.org/termanli/socket-connection-manager.svg?branch=master)

##Getting Start

        `ThreadPoolExecutor ioPool=new ThreadPoolExecutor(3, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(1000000));
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
         }});`
##Available configurations and default values,

Property Key | Description | Default Value
------------- |------------- |------------- 
CONNECTION_TIMEOUT | Timeout value for Connection | 1000







 
 