package kafka.core.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigUtils {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    public static <T> T getInstance(Class<T> targetClass, Object config)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException
    {
        if (null == config) {
            return null;
        }
        if ((config instanceof String)) {
            try
            {
                Class<?> realClass = Class.forName((String)config);
                if (targetClass.isAssignableFrom(realClass)) {
                    return (T)realClass.newInstance();
                }
                LOGGER.error("Specified class is not a ProduceProcessor: " + config);
                throw new ClassCastException("Specified class [" + config + "] is not assignable to target class [" + targetClass + "]");
            }
            catch (ClassNotFoundException e)
            {
                LOGGER.error("[HL_MESSAGE] Cannot find such class: " + config, e);
                throw e;
            }
            catch (IllegalAccessException|InstantiationException e)
            {
                LOGGER.error("[HL_MESSAGE] Cannot instantiate target: " + config, e);
                throw e;
            }
        }
        if (targetClass.isAssignableFrom(config.getClass())) {
            return (T)config;
        }
        LOGGER.error("Config is not a String or a instance of target class: " + config);
        throw new IllegalArgumentException("Specified class is not a ProduceProcessor: " + config);
    }

    public static Boolean getBoolean(Object config)
    {
        if (null == config) {
            return null;
        }
        if ((config instanceof String)) {
            return Boolean.valueOf(Boolean.parseBoolean((String)config));
        }
        if ((config instanceof Boolean)) {
            return (Boolean)config;
        }
        throw new IllegalArgumentException("Not a String or a Boolean");
    }

    public static String getString(Object config)
    {
        if (null == config) {
            return null;
        }
        if ((config instanceof String)) {
            return (String)config;
        }
        return config.toString();
    }

    public static Integer getInteger(Object config)
    {
        if (null == config) {
            return null;
        }
        if ((config instanceof String)) {
            return Integer.valueOf(Integer.parseInt((String)config));
        }
        if ((config instanceof Integer)) {
            return (Integer)config;
        }
        throw new IllegalArgumentException("Not a String or a Integer");
    }

    public static Long getLong(Object config)
    {
        if (null == config) {
            return null;
        }
        if ((config instanceof String)) {
            return Long.valueOf(Long.parseLong((String)config));
        }
        if ((config instanceof Long)) {
            return (Long)config;
        }
        throw new IllegalArgumentException("Not a String or a Long.");
    }


}