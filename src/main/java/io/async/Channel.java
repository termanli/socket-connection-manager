package io.async;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

/**
 * Created by j19li on 2017/11/27.
 */
public class Channel {
    private SocketConnectionManager scm;

    protected Channel(SocketConnectionManager scm) {
        this.scm = scm;
    }

    public int read() {
        byte[] result = new byte[1];
        int readCount = read(result, 0, 1);
        if (readCount >= 0) {
            return result[0];
        } else {
            return readCount;
        }
    }

    ;

    public void read(IntConsumer onComplete, Consumer<Throwable> onFailure) {
        byte[] result = new byte[1];
        read(result, 0, 1, (int i) -> {
            if (i >= 0) {
                onComplete.accept(result[0]);
            } else {
                onComplete.accept(i);
            }
        }, onFailure);
    }

    public int read(byte[] buffer, int pos, int maxLength) {
        SocketConnectionManager.ReadGuarder guarder = scm.read(this, buffer, pos, maxLength);
        if (guarder.isFinished(scm.getReadTimeOut() * 10)) {
            if (!guarder.isReadFailed()) {
                return guarder.getReadCount();
            } else {
                throw new AsyncIOException("Read Failed", guarder.getReadException());
            }
        } else {
            throw new AsyncIOException("read timeout");
        }
    }

    public void read(byte[] buffer, int pos, int maxLength, IntConsumer onComplete, Consumer<Throwable> onFailure) {
        scm.read(this, buffer, pos, maxLength, onComplete, onFailure);
    }

    public void write(int value) {
    }

    ;

    public void write(int value, Function<Void, Void> onComplete, Consumer<Throwable> onFailure) {
    }

    public void write(byte[] buffer, int off, int length) {
    }

    public void write(byte[] buffer, int off, int length, Function<Void, Void> onComplete, Consumer<Throwable> onFailure) {
    }

    public void write(byte[] buffer) {
        write(buffer, 0, buffer.length);
    }

    public void write(byte[] buffer, Function<Void, Void> onComplete, Consumer<Throwable> onFailure) {
        write(buffer, 0, buffer.length, onComplete, onFailure);
    }

    public void close() {
        scm.disconnect(this);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        scm.disconnect(this);
    }

}
