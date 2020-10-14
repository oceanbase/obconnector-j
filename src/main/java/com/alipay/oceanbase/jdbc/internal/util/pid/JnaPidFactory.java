package com.alipay.oceanbase.jdbc.internal.util.pid;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.platform.win32.Kernel32;
import java.util.function.Supplier;

public class JnaPidFactory {
  private static Supplier<String> instance;

  static {
    try {
      if (Platform.isLinux()) {
        // Linux pid implementation
        CLibrary.INSTANCE.getpid();
        instance = () -> String.valueOf(CLibrary.INSTANCE.getpid());
      } else if (Platform.isWindows()) {
        // Windows pid implementation
        try {
          Kernel32.INSTANCE.GetCurrentProcessId();
          instance =
              () -> {
                return String.valueOf(Kernel32.INSTANCE.GetCurrentProcessId());
              };
        } catch (Throwable cle) {
          // jna plateform jar's are not in classpath, no PID returned
          instance = () -> null;
        }
      } else {
        instance = () -> null;
      }

    } catch (Throwable cle) {
      // jna jar's are not in classpath, no PID returned
      instance = () -> null;
    }
  }

  /**
   * Factory method to avoid loading JNA classes every connection.
   *
   * @return factory that implement PID according to environment.
   */
  public static Supplier<String> getInstance() {
    return instance;
  }

  private interface CLibrary extends Library {
    CLibrary INSTANCE = Native.load("c", CLibrary.class);

    int getpid();
  }
}
