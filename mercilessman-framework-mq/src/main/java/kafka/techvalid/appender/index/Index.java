package kafka.techvalid.appender.index;

import kafka.techvalid.appender.file.MemoryMapFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
public class Index
        implements Closeable
{
    public static final int RECORD_MAX_COUNT = 4096;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int RECORD_SIZE = 20;
    public static final int HEADER_SIZE = 4;
    public static final int FILE_SIZE = 81924;
    private File file;
    private MemoryMapFile mmap;
    private ByteBuffer headerBuffer;
    private ByteBuffer bodyBuffer;
    private Integer count;
    private boolean closed;

    public Index(File file)
            throws IOException
    {
        this.file = file;
        this.mmap = new MemoryMapFile(file, 81924);
        this.headerBuffer = this.mmap.slice(0, 4);
        this.bodyBuffer = this.mmap.slice(4, 81920);
        this.headerBuffer.reset();
        this.count = Integer.valueOf(this.mmap.isCreated() ? 0 : this.headerBuffer.getInt());
    }

    public synchronized void addRecord(Record record)
    {
        this.headerBuffer.reset();
        int headCount = this.headerBuffer.getInt();
        if (headCount != this.count.intValue()) {
            this.count = Integer.valueOf(headCount);
        }
        int bodyOffset = this.count.intValue() * 20 + 4;
        this.bodyBuffer.position(bodyOffset);
        this.bodyBuffer.putInt(record.getOffset());
        this.bodyBuffer.putInt(record.getSize());
        this.bodyBuffer.putInt(record.getStatus());
        this.bodyBuffer.putLong(record.getTimestamp());
        this.headerBuffer.reset();
        this.headerBuffer.putInt((this.count = Integer.valueOf(this.count.intValue() + 1)).intValue());
    }

    public boolean isFull()
    {
        return getCount().intValue() >= 4096;
    }

    public synchronized void flush()
    {
        if (!this.closed) {
            this.mmap.flush();
        }
    }

    public synchronized Record get(int index)
    {
        if (!this.closed)
        {
            int bodyOffset = index * 20 + 4;
            int offset = this.bodyBuffer.getInt(bodyOffset);
            int size = this.bodyBuffer.getInt(bodyOffset + 4);
            int status = this.bodyBuffer.getInt(bodyOffset + 8);
            long timestamp = this.bodyBuffer.getLong(bodyOffset + 12);
            return new Record(offset, size, status, timestamp);
        }
        throw new RuntimeException("already closed");
    }

    public synchronized void setRecord(int index, int status, long timestamp)
    {
        if (!this.closed)
        {
            int bodyOffset = index * 20 + 4;
            this.bodyBuffer.position(bodyOffset + 8);
            this.bodyBuffer.putInt(status);
            this.bodyBuffer.putLong(timestamp);
        }
    }

    public Integer getCount()
    {
        return this.count;
    }

    public List<Record> loadAll()
    {
        List<Record> records = new ArrayList();
        for (int i = 0; i < this.count.intValue(); i++) {
            records.add(get(i));
        }
        return records;
    }

    public synchronized void close()
            throws IOException
    {
        this.closed = true;
        this.mmap.close();
    }
}

