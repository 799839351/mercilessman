package kafka.componet.recorder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import kafka.core.component.Recorder;
import kafka.core.consumer.ConsumeContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaRecorder implements Recorder<Map<TopicPartition, OffsetAndMetadata>> {
    public static final Log LOGGER = LogFactory.getLog(KafkaRecorder.class);
    private final ConcurrentHashMap<TopicPartition, PartitionTracker> batchStatus;

    public KafkaRecorder() {
        this.batchStatus = new ConcurrentHashMap();
    }

    public void init(Map<String, Object> config) {
    }

    public synchronized void record(ConsumeContext context) {
        long offset = getOffset(context);
        TopicPartition topicPartition = getTopicPartition(context);

        this.batchStatus.putIfAbsent(topicPartition, new PartitionTracker(topicPartition));
        ((PartitionTracker) this.batchStatus.get(topicPartition)).log(offset);
    }

    public synchronized boolean markCommitStatus(ConsumeContext context, boolean commitable) {
        if (!commitable) {
            return true;
        }
        PartitionTracker tracker = (PartitionTracker) this.batchStatus.get(getTopicPartition(context));
        if (tracker == null) {
            return false;
        }
        return tracker.commit(getOffset(context));
    }

    public static TopicPartition getTopicPartition(ConsumeContext context) {
        Map<String, Object> metadata = context.getConsumeMetadata();
        String topic = (String) metadata.get("kafka.topic");
        int partition = ((Integer) metadata.get("kafka.partition")).intValue();
        return new TopicPartition(topic, partition);
    }

    public static long getOffset(ConsumeContext context) {
        Map<String, Object> metadata = context.getConsumeMetadata();
        return ((Long) metadata.get("kafka.offset")).longValue();
    }

    public synchronized Map<TopicPartition, OffsetAndMetadata> getConsumeStatus() {
        Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap = new HashMap(8);
        for (Map.Entry<TopicPartition, PartitionTracker> entry : this.batchStatus.entrySet()) {
            LOGGER.debug("[HL_MESSAGE_KAFKA] " + entry.getKey() + " : " + ((PartitionTracker) entry.getValue()).firstOffset() + " --> " + ((PartitionTracker) entry.getValue()).lastOffset());

            Long offset = ((PartitionTracker) entry.getValue()).getCommitableOffset();
            if (offset != null) {
                partitionOffsetMap.put(entry.getKey(), new OffsetAndMetadata(offset.longValue()));
            }
        }
        LOGGER.debug("[HL_MESSAGE_KAFKA] Committing: " + Arrays.asList(new Set[]{partitionOffsetMap.entrySet()}));
        return partitionOffsetMap;
    }

    public synchronized void reset() {
        this.batchStatus.clear();
    }
}

