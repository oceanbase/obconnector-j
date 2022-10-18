package com.oceanbase.jdbc.internal.failover.BlackList;

import java.util.HashMap;

import com.oceanbase.jdbc.internal.failover.BlackList.append.AppendStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;

public class BlackListConfig {
    RemoveStrategy removeStrategy;
    AppendStrategy appendStrategy;
    HashMap<String,String>  removeStrategyConfigs = new HashMap<>();
    HashMap<String,String>  appendStrategyConfigs = new HashMap<>();

    public HashMap<String, String> getRemoveStrategyConfigs() {
        return removeStrategyConfigs;
    }

    public void setRemoveStrategyConfigs(HashMap<String, String> removeStrategyConfigs) {
        this.removeStrategyConfigs = removeStrategyConfigs;
    }

    public HashMap<String, String> getAppendStrategyConfigs() {
        return appendStrategyConfigs;
    }

    public void setAppendStrategyConfigs(HashMap<String, String> appendStrategyConfigs) {
        this.appendStrategyConfigs = appendStrategyConfigs;
    }

    public RemoveStrategy getRemoveStrategy() {
        return removeStrategy;
    }

    public BlackListConfig() {
        appendStrategyConfigs.put("NAME","NORMAL");
        removeStrategyConfigs.put("NAME","TIMEOUT");
        removeStrategyConfigs.put("TIMEOUT","50");
    }

    public void setRemoveStrategy(RemoveStrategy removeStrategy) {
        this.removeStrategy = removeStrategy;
    }

    @Override
    public String toString() {
        return "BlackListConfig{" + "removeStrategy=" + removeStrategy + ", appendStrategy="
               + appendStrategy + '}';
    }

    public AppendStrategy getAppendStrategy() {
        return appendStrategy;
    }

    public void setAppendStrategy(AppendStrategy appendStrategy) {
        this.appendStrategy = appendStrategy;
    }


}
