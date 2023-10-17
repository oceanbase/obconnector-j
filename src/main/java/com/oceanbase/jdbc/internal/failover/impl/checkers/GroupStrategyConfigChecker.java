package com.oceanbase.jdbc.internal.failover.impl.checkers;

import java.util.*;
import java.util.HashSet;

import com.oceanbase.jdbc.internal.failover.impl.checkers.ConfigChecker;

public class GroupStrategyConfigChecker implements ConfigChecker {
    HashSet<String> keySet = new HashSet<>(Arrays.asList("NAME"));
    HashSet<String> namesSet = new HashSet<>(Arrays.asList("ROTATION"));


    @Override
    public boolean isValid(HashMap<String, String> config) {
        return keySet.containsAll(config.keySet()) && namesSet.contains(config.get("NAME"));
    }
}
