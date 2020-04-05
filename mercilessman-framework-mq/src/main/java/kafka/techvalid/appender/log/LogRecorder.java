package kafka.techvalid.appender.log;


public abstract class LogRecorder
{
    private static LogRecorder logRecorder;

    protected static void setInstance(LogRecorder logRecorder)
    {
        logRecorder = logRecorder;
    }

    public static LogRecorder getInstance()
    {
        return logRecorder;
    }

    public abstract void info(Object paramObject);

    public abstract void info(Object paramObject, Throwable paramThrowable);

    public abstract void warn(Object paramObject);

    public abstract void warn(Object paramObject, Throwable paramThrowable);

    public abstract void error(Object paramObject);

    public abstract void error(Object paramObject, Throwable paramThrowable);

    public abstract void cleanupIncrement();

    public abstract long getCleanUpNum();
}

