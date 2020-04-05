package kafka.componet.recorder;


import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.kafka.common.TopicPartition;

public class PartitionTracker {
    private final TopicPartition topicPartition;
    private final TreeMap<Long, Boolean> progress;

    public PartitionTracker(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        this.progress = new TreeMap();
    }

    public synchronized void log(long offset) {
        this.progress.put(Long.valueOf(offset), Boolean.valueOf(false));
    }

    public synchronized boolean commit(long offset) {
        if (this.progress.containsKey(Long.valueOf(offset))) {
            this.progress.put(Long.valueOf(offset), Boolean.valueOf(true));
            return true;
        }
        return false;
    }

    public synchronized Long getCommitableOffset() {
        Long lastCommitted = null;
        for (Map.Entry<Long, Boolean> status : this.progress.entrySet()) {
            if (!((Boolean) status.getValue()).booleanValue()) {
                break;
            }
            lastCommitted = (Long) status.getKey();
        }
        return lastCommitted == null ? null : Long.valueOf(lastCommitted.longValue() + 1L);
    }

    public TopicPartition getTopicPartition() {
        return this.topicPartition;
    }

    public long firstOffset() {
        return ((Long) this.progress.firstKey()).longValue();
    }

    public long lastOffset() {
        return ((Long) this.progress.lastKey()).longValue();
    }
}

