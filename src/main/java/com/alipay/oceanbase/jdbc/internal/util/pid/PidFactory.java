package com.alipay.oceanbase.jdbc.internal.util.pid;

import java.lang.reflect.Method;
import java.util.function.Supplier;

public class PidFactory {
  private static Supplier<String> instance;

  static {
    try {
      // if java 9+
      Class<?> processHandle = Class.forName("java.lang.ProcessHandle");
      instance =
          () -> {
            try {
              Method currentProcessMethod = processHandle.getMethod("current");
              Object currentProcess = currentProcessMethod.invoke(null);
              Method pidMethod = processHandle.getMethod("pid");
              return String.valueOf(pidMethod.invoke(currentProcess));
            } catch (Throwable throwable) {
              return null;
            }
          };
    } catch (Throwable cle) {
      try {
        instance = JnaPidFactory.getInstance();
      } catch (Throwable throwable) {
        instance = () -> null;
      }
    }
  }

  public static Supplier<String> getInstance() {
    return instance;
  }
}
