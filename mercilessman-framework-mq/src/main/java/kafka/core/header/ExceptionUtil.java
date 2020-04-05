package kafka.core.header;

import kafka.core.Message;

public class ExceptionUtil
{
    public static final String CONSUME_EXCEPTION_HEADER = "x_consume_exception";

    public static void putConsumeException(Message<?> message, Exception e)
    {
        message.putHeader("x_consume_exception", e.toString());
    }
}