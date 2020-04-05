package kafka.core.header;

import kafka.core.Message;

public class DelayUtil
{
    public static final String CONSUME_DELAY_SEC = "x_consume_delay_sec";

    public static void setConsumeDelay(Message<?> message, long second)
    {
        message.putHeader("x_consume_delay_sec", Long.toString(second));
    }

    public static long getConsumeDelay(Message<?> message)
    {
        String consumeDelayMsString = message.getHeader("x_consume_delay_sec");
        return consumeDelayMsString == null ? 0L : Long.parseLong(consumeDelayMsString);
    }

    public static long removeConsumeDelay(Message<?> message)
    {
        String consumeDelayMsString = message.removeHeader("x_consume_delay_sec");
        return consumeDelayMsString == null ? 0L : Long.parseLong(consumeDelayMsString);
    }

    public static boolean hasConsumeDelay(Message<?> message)
    {
        return message.headers().containsKey("x_consume_delay_sec");
    }
}

