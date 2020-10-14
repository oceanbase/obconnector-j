package com.alipay.oceanbase.jdbc.internal.logging;

public class LoggerFactory {

    private static final Logger NO_LOGGER = new NoLogger();
    private static Boolean      hasToLog  = null;

    /**
     * Initialize factory.
     *
     * @param mustLog indicate if must initiate Slf4j log
     */
    @SuppressWarnings("unchecked")
    public static void init(boolean mustLog) {
        if ((hasToLog == null || hasToLog != mustLog) && mustLog) {
            synchronized (LoggerFactory.class) {
                if (hasToLog == null || hasToLog != mustLog) {
                    try {
                        Class.forName("org.slf4j.LoggerFactory");
                        hasToLog = Boolean.TRUE;
                    } catch (ClassNotFoundException classNotFound) {
                        System.out.println("Logging cannot be activated, missing slf4j dependency");
                        hasToLog = Boolean.FALSE;
                    }
                }
            }
        }
    }

    /**
     * Initialize logger.
     *
     * @param clazz initiator class
     * @return logger
     */
    public static Logger getLogger(Class<?> clazz) {
        if (hasToLog != null && hasToLog) {
            return new Slf4JLogger(org.slf4j.LoggerFactory.getLogger(clazz));
        } else {
            return NO_LOGGER;
        }
    }
}
