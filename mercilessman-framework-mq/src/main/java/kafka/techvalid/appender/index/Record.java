package kafka.techvalid.appender.index;


public class Record
{
    private int offset;
    private int size;
    private int status;
    private long timestamp;

    public Record(int offset, int size, int status, long timestamp)
    {
        this.offset = offset;
        this.size = size;
        this.status = status;
        this.timestamp = timestamp;
    }

    public int getOffset()
    {
        return this.offset;
    }

    public void setOffset(int offset)
    {
        this.offset = offset;
    }

    public int getSize()
    {
        return this.size;
    }

    public void setSize(int size)
    {
        this.size = size;
    }

    public int getStatus()
    {
        return this.status;
    }

    public void setStatus(int status)
    {
        this.status = status;
    }

    public long getTimestamp()
    {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }
}

