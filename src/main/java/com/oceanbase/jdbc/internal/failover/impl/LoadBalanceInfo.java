package com.oceanbase.jdbc.internal.failover.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.oceanbase.jdbc.internal.failover.BlackList.*;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.*;
import com.oceanbase.jdbc.internal.failover.utils.Consts;
import com.oceanbase.jdbc.internal.util.constant.HaMode;

public class LoadBalanceInfo {

    HaMode               haMode;
    String               serviceName;
    GroupBalanceStrategy groupBalanceStrategy;
    HashMap<String,String> groupBalanceStrategyConfigs = new HashMap<>();
    BalanceStrategy      balanceStrategy;     // Global Load Balancing Strategy
    HashMap<String,String> balanceStrategyConfigs = new HashMap<>();

    BlackListConfig      blackListConfig;     //  Global Load Balancing Black List Config Information

    int retryAllDowns;

    List<LoadBalanceAddressList> groups;

    public HashMap<String, String> getGroupBalanceStrategyConfigs() {
        return groupBalanceStrategyConfigs;
    }

    public void setGroupBalanceStrategyConfigs(HashMap<String, String> groupBalanceStrategyConfigs) {
        this.groupBalanceStrategyConfigs = groupBalanceStrategyConfigs;
    }

    public HashMap<String, String> getBalanceStrategyConfigs() {
        return balanceStrategyConfigs;
    }

    public void setBalanceStrategyConfigs(HashMap<String, String> balanceStrategyConfigs) {
        this.balanceStrategyConfigs = balanceStrategyConfigs;
    }

    public LoadBalanceInfo() {
        this.groups  = new ArrayList<>();
        groupBalanceStrategy = new GroupRotationStrategy();
        blackListConfig = new BlackListConfig();
        balanceStrategy = new RandomStrategy();
        groupBalanceStrategyConfigs.put(Consts.NAME,Consts.ROTATION);
        balanceStrategyConfigs.put(Consts.NAME,Consts.RANDOM);
        retryAllDowns = 120;
    }

    public LoadBalanceInfo(GroupBalanceStrategy groupBalanceStrategy,
                           BalanceStrategy balanceStrategy, BlackListConfig blackListConfig,
                           List<LoadBalanceAddressList> groups) {
        this.groupBalanceStrategy = groupBalanceStrategy;
        this.balanceStrategy = balanceStrategy;
        this.blackListConfig = blackListConfig;
        this.groups = groups;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String toString() {
        return "LoadBalanceInfo{" + "haMode=" + haMode + ", serviceName='" + serviceName + '\''
               + ", groupBalanceStrategy=" + groupBalanceStrategy + ", balanceStrategy="
               + balanceStrategy + ", blackListConfig=" + blackListConfig + ", retryAllDowns="
               + retryAllDowns + ", groups=" + groups + '}';
    }

    public HaMode getHaMode() {
        return haMode;
    }

    public void setHaMode(HaMode haMode) {
        this.haMode = haMode;
    }

    public GroupBalanceStrategy getGroupBalanceStrategy() {
        return groupBalanceStrategy;
    }

    public BalanceStrategy getBalanceStrategy() {
        return balanceStrategy;
    }

    public BlackListConfig getBlackListConfig() {
        return blackListConfig;
    }

    public int getRetryAllDowns() {
        return retryAllDowns;
    }

    public List<LoadBalanceAddressList> getGroups() {
        return groups;
    }

    public void setRetryAllDowns(int retryAllDowns) {
        this.retryAllDowns = retryAllDowns;
    }


    public void setBlackListConfig(BlackListConfig blackListConfig) {
        this.blackListConfig = blackListConfig;
    }

    public void setGroups(List<LoadBalanceAddressList> groups) {
        this.groups = groups;
    }


    public void setGroupBalanceStrategy(GroupBalanceStrategy groupBalanceStrategy) {
        this.groupBalanceStrategy = groupBalanceStrategy;
    }

    public void setBalanceStrategy(BalanceStrategy balanceStrategy) {
        this.balanceStrategy = balanceStrategy;
    }

}
