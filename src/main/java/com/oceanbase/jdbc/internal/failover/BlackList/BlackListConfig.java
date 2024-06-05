package com.oceanbase.jdbc.internal.failover.BlackList;

import java.util.HashMap;

import com.oceanbase.jdbc.internal.failover.BlackList.append.AppendStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.append.NormalAppend;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.TimeoutRecover;

public class BlackListConfig {
    RemoveStrategy          removeStrategy;
    AppendStrategy          appendStrategy;
    HashMap<String, String> removeStrategyConfigs;
    HashMap<String, String> appendStrategyConfigs;

    public BlackListConfig() {
        appendStrategyConfigs = new HashMap<>();
        appendStrategyConfigs.put("NAME","NORMAL");
        removeStrategyConfigs = new HashMap<>();
        removeStrategyConfigs.put("NAME","TIMEOUT");
        removeStrategyConfigs.put("TIMEOUT","50");
    }

    public BlackListConfig(boolean byDefault) {
        if (byDefault) {
            appendStrategy = new NormalAppend();
            removeStrategy = new TimeoutRecover();
        }
    }

    @Override
    public String toString() {
        return "BlackListConfig{" + "removeStrategy=" + removeStrategy + ", appendStrategy="
               + appendStrategy + '}';
    }

    public String toJson() {
        StringBuilder json = new StringBuilder("\"OBLB_BLACKLIST\":{\n");
        boolean atLeastOne = false;

        if (removeStrategy != null) {
            atLeastOne = true;
            json.append(removeStrategy.toJson());
        }
        if (appendStrategy != null) {
            if (atLeastOne) {
                json.append(",");
            }
            json.append(appendStrategy.toJson());
        }

        json.append("}");
        return json.toString();
    }

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

    public void setRemoveStrategy(RemoveStrategy removeStrategy) {
        this.removeStrategy = removeStrategy;
    }

    public AppendStrategy getAppendStrategy() {
        return appendStrategy;
    }

    public void setAppendStrategy(AppendStrategy appendStrategy) {
        this.appendStrategy = appendStrategy;
    }

}
