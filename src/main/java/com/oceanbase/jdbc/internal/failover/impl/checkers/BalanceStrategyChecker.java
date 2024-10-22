package com.oceanbase.jdbc.internal.failover.impl.checkers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class BalanceStrategyChecker implements ConfigChecker {
    HashSet<String> keySet = new HashSet<>(Arrays.asList("NAME"));

    HashSet<String> namesSet = new HashSet<>(Arrays.asList("ROTATION","RANDOM","SERVERAFFINITY","DEFAULT"));
    @Override
    public boolean isValid(HashMap<String, String> config) {
        return keySet.containsAll(config.keySet()) && namesSet.contains(config.get("NAME"));
    }
}
