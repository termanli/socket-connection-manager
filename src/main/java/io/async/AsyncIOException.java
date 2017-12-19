package io.async;

/**
 * Created by j19li on 2017/11/27.
 */
public class AsyncIOException extends RuntimeException {

    public AsyncIOException(Throwable ex) {
        super(ex);
    }
    public AsyncIOException(String msg, Throwable ex){
        super(msg,ex);
    }
    public AsyncIOException(String msg){
        super(msg);
    }
}
