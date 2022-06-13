package com.oceanbase.jdbc.internal.failover.impl.checkers;

import java.util.HashMap;

public interface ConfigChecker {
    boolean isValid(HashMap<String, String> config);
}
