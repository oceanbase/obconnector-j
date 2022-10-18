package com.oceanbase.jdbc.internal.failover.impl.checkers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class BlackListStrategyAppendChecker implements ConfigChecker {
    HashSet<String> keySet = new HashSet<>(Arrays.asList("NAME","RETRYTIMES","DURATION"));
    HashSet<String> namesSet = new HashSet<>(Arrays.asList("RETRYDURATION","NORMAL"));
    @Override
    public boolean isValid(HashMap<String, String> config) {
        if( keySet.containsAll(config.keySet()) && namesSet.contains(config.get("NAME")) ){
            if(config.get("NAME") == "RETRYDURATION") {
                String retrytimes = config.get("RETRYTIMES");
                try {
                    int t = Integer.parseInt(retrytimes);
                    if (t < 0) {
                        return false;
                    }
                } catch (NumberFormatException e) {
                    return false;
                }
                String duration = config.get("DURATION");
                try {
                    int t = Integer.parseInt(duration);
                    if (t < 0) {
                        return false;
                    } else {
                        return true;
                    }
                } catch (NumberFormatException e) {
                    return false;
                }
            } else {
                return  true;
            }
        }
        return false;
    }
}
