package com.alipay.oceanbase.jdbc.internal.logging;

public class NoLogger implements Logger {

    public boolean isTraceEnabled() {
        return false;
    }

    public void trace(String msg) {
        // do nothing
    }

    public void trace(String format, Object arg) {
        // do nothing
    }

    public void trace(String format, Object arg1, Object arg2) {
        // do nothing
    }

    public void trace(String format, Object... arguments) {
        // do nothing
    }

    public void trace(String msg, Throwable throwable) {
        // do nothing
    }

    public boolean isDebugEnabled() {
        return false;
    }

    public void debug(String msg) {
        // do nothing
    }

    public void debug(String format, Object arg) {
        // do nothing
    }

    public void debug(String format, Object arg1, Object arg2) {
        // do nothing
    }

    public void debug(String format, Object... arguments) {
        // do nothing
    }

    public void debug(String msg, Throwable throwable) {
        // do nothing
    }

    public boolean isInfoEnabled() {
        return false;
    }

    public void info(String msg) {
        // do nothing
    }

    public void info(String format, Object arg) {
        // do nothing
    }

    public void info(String format, Object arg1, Object arg2) {
        // do nothing
    }

    public void info(String format, Object... arguments) {
        // do nothing
    }

    public void info(String msg, Throwable throwable) {
        // do nothing
    }

    public boolean isWarnEnabled() {
        return false;
    }

    public void warn(String msg) {
        // do nothing
    }

    public void warn(String format, Object arg) {
        // do nothing
    }

    public void warn(String format, Object... arguments) {
        // do nothing
    }

    public void warn(String format, Object arg1, Object arg2) {
        // do nothing
    }

    public void warn(String msg, Throwable throwable) {
        // do nothing
    }

    public boolean isErrorEnabled() {
        return false;
    }

    public void error(String msg) {
        // do nothing
    }

    public void error(String format, Object arg) {
        // do nothing
    }

    public void error(String format, Object arg1, Object arg2) {
        // do nothing
    }

    public void error(String format, Object... arguments) {
        // do nothing
    }

    public void error(String msg, Throwable throwable) {
        // do nothing
    }
}
