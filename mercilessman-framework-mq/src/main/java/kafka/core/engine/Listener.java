package kafka.core.engine;

public interface Listener<T extends Event>
{
     void process(T paramT);

     Class<? extends Event>[] getListeningEvents();
}
