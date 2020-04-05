package kafka.core.header;


import kafka.core.Message;
import org.apache.commons.lang3.StringUtils;

public class ReproduceUtil {
    public static final String RETRY_FLAG = "x_retry_flag";
    public static final String DLQ_FLAG = "x_dlq_flag";
    public static final String RETRY_BIZ_FAILED = "x_retry_biz_failed";
    public static final String RETRY_SYS_FAILED = "x_retry_sys_failed";
    public static final String CONSUMED_BY = "x_consumed_by";

    public static void setRetryFlag(Message<?> message, boolean isRetry) {
        message.putHeader("x_retry_flag", Boolean.toString(isRetry));
    }

    public static boolean removeRetryFlag(Message<?> message) {
        boolean retryFlag = getRetryFlag(message);
        message.removeHeader("x_retry_flag");
        return retryFlag;
    }

    public static boolean getRetryFlag(Message<?> message) {
        String retryFlagString = message.getHeader("x_retry_flag");
        return (!StringUtils.isEmpty(retryFlagString)) && (Boolean.parseBoolean(retryFlagString));
    }

    public static void setDlqFlag(Message<?> message, boolean isDlq) {
        message.putHeader("x_dlq_flag", Boolean.toString(isDlq));
    }

    public static boolean removeDlqFlag(Message<?> message) {
        boolean retryFlag = getRetryFlag(message);
        message.removeHeader("x_dlq_flag");
        return retryFlag;
    }

    public static boolean getDlqFlag(Message<?> message) {
        String dlqFlagString = message.getHeader("x_dlq_flag");
        return (!StringUtils.isEmpty(dlqFlagString)) && (Boolean.parseBoolean(dlqFlagString));
    }

    public static int logBizFailure(Message<?> message) {
        String bizFailString = message.getHeader("x_retry_biz_failed");
        if (null == bizFailString) {
            message.putHeader("x_retry_biz_failed", Integer.toString(1));
            return 1;
        }
        int bizFail = Integer.parseInt(bizFailString);
        message.putHeader("x_retry_biz_failed", Integer.toString(bizFail + 1));
        return bizFail + 1;
    }

    public static int getBizFailure(Message<?> message) {
        String bizFailString = message.getHeader("x_retry_biz_failed");
        if (null == bizFailString) {
            return 0;
        }
        return Integer.parseInt(bizFailString);
    }

    public static void resetBizFailure(Message<?> message) {
        message.removeHeader("x_retry_biz_failed");
    }

    public static int logSysFailure(Message<?> message) {
        String sysFailString = message.getHeader("x_retry_sys_failed");
        if (null == sysFailString) {
            message.putHeader("x_retry_sys_failed", Integer.toString(1));
            return 1;
        }
        int bizFail = Integer.parseInt(sysFailString);
        message.putHeader("x_retry_sys_failed", Integer.toString(bizFail + 1));
        return bizFail + 1;
    }

    public static int getSysFailure(Message<?> message) {
        String sysFailString = message.getHeader("x_retry_sys_failed");
        if (null == sysFailString) {
            return 0;
        }
        return Integer.parseInt(sysFailString);
    }

    public static void resetSysFailure(Message<?> message) {
        message.removeHeader("x_retry_sys_failed");
    }

    public static void setConsumedBy(Message<?> message, String consumer) {
        message.putHeader("x_consumed_by", consumer);
    }

    public static String getConsumedBy(Message<?> message) {
        return message.getHeader("x_consumed_by");
    }

    public static String removeConsumedBy(Message<?> message) {
        return message.removeHeader("x_consumed_by");
    }
}

