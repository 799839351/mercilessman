package kafka.techvalid.appender.log;

import kafka.techvalid.appender.index.Record;

import java.nio.ByteBuffer;

public class MultipleLogItem extends LogItem {
    private long globalIndex;

    public MultipleLogItem(Record record, ByteBuffer data, long fileId, long index) {
        super(record, data, index);
        this.globalIndex = (index + fileId * 4096L);
    }

    public long getIndex() {
        return this.globalIndex;
    }
}
