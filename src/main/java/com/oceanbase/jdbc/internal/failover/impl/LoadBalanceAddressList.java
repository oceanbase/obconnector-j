package com.oceanbase.jdbc.internal.failover.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.BalanceStrategy;

public class LoadBalanceAddressList {
    public BalanceStrategy              balanceStrategy;
    public BlackListConfig              blackListConfig;       //not currently in use
    HashMap<String, String>             balanceStrategyConfigs;
    public List<LoadBalanceHostAddress> addressList;

    public LoadBalanceAddressList() {
        addressList = new ArrayList<>();
        balanceStrategyConfigs = new HashMap<>();
        balanceStrategyConfigs.put("NAME","RANDOM");
    }

    public LoadBalanceAddressList(BalanceStrategy balanceStrategy, BlackListConfig blackListConfig,
                                  List<LoadBalanceHostAddress> addressList) {
        this.balanceStrategy = balanceStrategy;
        this.blackListConfig = blackListConfig;
        this.addressList = addressList;
    }

    @Override
    public String toString() {
        return "LoadBalanceAddressList{" + "balanceStrategy=" + balanceStrategy
               + ", blackListConfig=" + blackListConfig + ", addressList=" + addressList + '}';
    }

    public String toJson() {
        StringBuilder json = new StringBuilder();
        boolean atLeastOneKey = false;

        if (balanceStrategy != null) {
            atLeastOneKey = true;
            json.append(balanceStrategy.toJson());
        }
        if (blackListConfig != null) {
            if (atLeastOneKey) {
                json.append(",");
            } else {
                atLeastOneKey = true;
            }
            json.append(blackListConfig.toJson());
        }
        if (addressList != null && addressList.size() > 0) {
            if (atLeastOneKey) {
                json.append(",");
            }
            json.append("\"ADDRESS\": [");

            boolean atLeastOneAddress = false;
            for (LoadBalanceHostAddress address : addressList) {
                if (atLeastOneAddress) {
                    json.append(",");
                } else {
                    atLeastOneAddress = true;
                }
                json.append("{").append(address.toJson()).append("}");
            }
            json.append("]");
        }

        return json.toString();
    }

    public HashMap<String, String> getBalanceStrategyConfigs() {
        return balanceStrategyConfigs;
    }

    public void setBalanceStrategyConfigs(HashMap<String, String> balanceStrategyConfigs) {
        this.balanceStrategyConfigs = balanceStrategyConfigs;
    }

    public BalanceStrategy getBalanceStrategy() {
        return balanceStrategy;
    }

    public void setBalanceStrategy(BalanceStrategy balanceStrategy) {
        this.balanceStrategy = balanceStrategy;
    }

    public BlackListConfig getBlackListConfig() {
        return blackListConfig;
    }

    public void setBlackListConfig(BlackListConfig blackListConfig) {
        this.blackListConfig = blackListConfig;
    }

    public List<LoadBalanceHostAddress> getAddressList() {
        return addressList;
    }

    public void setAddressList(List<LoadBalanceHostAddress> addressList) {
        this.addressList = addressList;
    }

    public List<HostAddress> convertToHostAddressList() {
        List<HostAddress> ret = new ArrayList<>();
        for(LoadBalanceHostAddress key : addressList) {
            ret.add(key);
        }
        return  ret ;
    }
}
