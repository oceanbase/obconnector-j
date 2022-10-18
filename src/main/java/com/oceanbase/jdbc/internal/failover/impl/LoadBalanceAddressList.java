package com.oceanbase.jdbc.internal.failover.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.BalanceStrategy;

public class LoadBalanceAddressList {
    public BalanceStrategy              balanceStrategy;
    public BlackListConfig              blackListConfig; //not currently in use
    public List<LoadBalanceHostAddress> addressList;

    HashMap<String,String> balanceStrategyConfigs = new HashMap<>();

    public HashMap<String, String> getBalanceStrategyConfigs() {
        return balanceStrategyConfigs;
    }

    public void setBalanceStrategyConfigs(HashMap<String, String> balanceStrategyConfigs) {
        this.balanceStrategyConfigs = balanceStrategyConfigs;
    }

    @Override
    public String toString() {
        return "LoadBalanceAddressList{" + "balanceStrategy=" + balanceStrategy
               + ", blackListConfig=" + blackListConfig + ", addressList=" + addressList + '}';
    }

    public LoadBalanceAddressList() {
        this.addressList = new ArrayList<>();
        balanceStrategyConfigs.put("NAME","RANDOM");
    }
    public LoadBalanceAddressList(BalanceStrategy balanceStrategy, BlackListConfig blackListConfig,
                                  List<LoadBalanceHostAddress> addressList) {
        this.balanceStrategy = balanceStrategy;
        this.blackListConfig = blackListConfig;
        this.addressList = addressList;
        balanceStrategyConfigs.put("NAME","RANDOM");
    }

    public BalanceStrategy getBalanceStrategy() {
        return balanceStrategy;
    }

    public BlackListConfig getBlackListConfig() {
        return blackListConfig;
    }

    public List<LoadBalanceHostAddress> getAddressList() {
        return addressList;
    }


    public void setBalanceStrategy(BalanceStrategy balanceStrategy) {
        this.balanceStrategy = balanceStrategy;
    }

    public void setBlackListConfig(BlackListConfig blackListConfig) {
        this.blackListConfig = blackListConfig;
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
