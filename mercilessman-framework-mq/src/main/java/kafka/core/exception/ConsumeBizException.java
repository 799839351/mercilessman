package kafka.core.exception;


public class ConsumeBizException  extends ConsumeException
{
    public ConsumeBizException() {}

    public ConsumeBizException(String message)
    {
        super(message);
    }

    public ConsumeBizException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ConsumeBizException(Throwable cause)
    {
        super(cause);
    }
}
