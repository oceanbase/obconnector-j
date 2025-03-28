package com.oceanbase.jdbc.internal.failover.impl.checkers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class BlackListStrategyRemoveChecker implements ConfigChecker {
    HashSet<String> keySet = new HashSet<>(Arrays.asList("NAME","TIMEOUT"));
    HashSet<String> namesSet = new HashSet<>(Arrays.asList("TIMEOUT"));
    @Override
    public boolean isValid(HashMap<String, String> config) {
        if(keySet.containsAll(config.keySet()) && namesSet.contains(config.get("NAME")) ){
            String timeout = config.get("TIMEOUT");
            try {
                int t = Integer.parseInt(timeout);
                if(t<0) {
                    return false;
                } else {
                    return true;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }
}
