package kafka.techvalid.appender.log;


import kafka.techvalid.appender.util.FullException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract interface Log extends Closeable {
    public abstract long append(ByteBuffer paramByteBuffer, long paramLong)
            throws IOException, FullException;

    public abstract LogItem get(long paramLong);

    public abstract void setStatus(long paramLong1, int paramInt, long paramLong2)
            throws IOException;

    public abstract void close()
            throws IOException;

    public abstract long flush()
            throws IOException;

    public abstract Long getMinUncommit()
            throws IOException;

    public abstract List<LogItem> loadUncommitted(int paramInt)
            throws IOException;

    public abstract void ensure(long paramLong)
            throws IOException;
}

