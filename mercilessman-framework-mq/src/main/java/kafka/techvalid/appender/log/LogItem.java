package kafka.techvalid.appender.log;


import kafka.techvalid.appender.index.Record;

import java.nio.ByteBuffer;

public class LogItem
{
    private Record record;
    private ByteBuffer data;
    private long index;

    public LogItem(Record record, ByteBuffer data, long index)
    {
        this.record = record;
        this.data = data;
        this.index = index;
    }

    public Record getRecord()
    {
        return this.record;
    }

    public void setRecord(Record record)
    {
        this.record = record;
    }

    public ByteBuffer getData()
    {
        return this.data;
    }

    public void setData(ByteBuffer data)
    {
        this.data = data;
    }

    public void setIndex(long index)
    {
        this.index = index;
    }

    public long getIndex()
    {
        return this.index;
    }
}
