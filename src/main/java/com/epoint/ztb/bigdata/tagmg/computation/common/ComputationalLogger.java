package com.epoint.ztb.bigdata.tagmg.computation.common;

import org.apache.log4j.Logger;

public class ComputationalLogger
{
    private Logger logger;

    private boolean isDebug;

    public ComputationalLogger(Logger logger) {
        super();
        this.logger = logger;
    }

    public static ComputationalLogger getLogger(final Class<?> clazz) {
        return new ComputationalLogger(Logger.getLogger(clazz));
    }

    public void setDebug(boolean isDebug) {
        this.isDebug = isDebug;
    }

    public void debug(Object message) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.debug(message);
    }

    public void debug(Object message, Throwable t) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.debug(message, t);
    }

    public void error(Object message) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.error(message);
    }

    public void error(Object message, Throwable t) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.error(message, t);
    }

    public void warn(Object message) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.warn(message);
    }

    public void warn(Object message, Throwable t) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.warn(message, t);
    }

    public void info(Object message) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.info(message);
    }

    public void info(Object message, Throwable t) {
        if (isDebug) {
            System.out.println(message);
        }
        logger.info(message, t);
    }

    public static void print(Object message) {
        System.out.println(message);
    }

    public static void print(Object message, Throwable t) {
        System.out.println(message);
        t.printStackTrace();
    }
}
