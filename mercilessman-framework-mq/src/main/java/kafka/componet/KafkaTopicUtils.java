package kafka.componet;


import java.util.Collection;
import java.util.regex.Pattern;

public class KafkaTopicUtils {
    public static final String RETRY_SUFFIX = "-RETRY";
    public static final String DEAD_LETTER_SUFFIX = "-DEAD";
    public static final String DELAY_SUFFIX = "-DELAY";

    public static String getDLQTopic(String topic) {
        return topic + "-DEAD";
    }

    public static String getRetryTopic(String topic, String group, Long delaySec) {
        return topic + "-RETRY" + "-" + group + "-" + delaySec.toString();
    }

    public static String getDelayTopic(String topic, Long delaySec) {
        return topic + "-DELAY" + "-" + delaySec;
    }

    public static String getCombinedTopicReg(String topic, String group) {
        return String.format("^%s((%s|%s-%s)-[0-9]+)?$", new Object[]{topic, "-DELAY", "-RETRY", group});
    }

    public static Pattern getTopicPattern(String topic, String group) {
        return Pattern.compile(getCombinedTopicReg(topic, group));
    }

    public static Pattern getTopicPattern(Collection<String> topics, String group) {
        StringBuilder builder = new StringBuilder();
        for (String topic : topics) {
            builder.append(getCombinedTopicReg(topic, group)).append("|");
        }
        builder.deleteCharAt(builder.lastIndexOf("|"));
        return Pattern.compile(builder.toString());
    }
}
