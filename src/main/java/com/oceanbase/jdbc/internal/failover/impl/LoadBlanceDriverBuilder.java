package com.oceanbase.jdbc.internal.failover.impl;

import java.sql.SQLException;
import java.util.HashMap;

import com.oceanbase.jdbc.internal.failover.BlackList.*;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.*;
import com.oceanbase.jdbc.internal.failover.impl.checkers.*;

public class LoadBlanceDriverBuilder implements Builder {
    private LoadBalanceDriver loadBalanceDriver = null;
    ConfigChecker             checker           = null;

    public LoadBlanceDriverBuilder(LoadBalanceInfo loadBalanceInfo) {
        this.loadBalanceInfo = loadBalanceInfo;
        reset();
    }

    LoadBalanceInfo loadBalanceInfo = null;

    public LoadBlanceDriverBuilder(LoadBalanceDriver loadBalanceDriver) {
        this.loadBalanceDriver = loadBalanceDriver;
        reset();
    }

    public LoadBlanceDriverBuilder() {
        reset();
    }

    @Override
    public void reset() {
        loadBalanceDriver = new LoadBalanceDriver();
    }

    @Override
    public void buildGlobalConfigs() {
        loadBalanceDriver.setLoadBalanceInfo(loadBalanceInfo);
    }

    @Override
    public void buildGroupStrategy() throws SQLException {
        HashMap<String, String> map = loadBalanceInfo.getGroupBalanceStrategyConfigs();
        GroupBalanceStrategy groupBalanceStrategy = null;
        String name = map.get("NAME");
        checker = new GroupStrategyConfigChecker();
        if (!checker.isValid(map)) {
            throw new SQLException("groupBalanceStrategy config incorrect：" + map);
        }
        switch (name) {
            case "ROTATION":
            default:
                groupBalanceStrategy = new GroupRotationStrategy();
                break;
        }
        loadBalanceInfo.setGroupBalanceStrategy(groupBalanceStrategy);
    }

    @Override
    public void buildBlackListStrategy() throws SQLException {
        BlackListConfig blackListConfig = loadBalanceInfo.getBlackListConfig();
        String name = blackListConfig.getRemoveStrategyConfigs().get("NAME");
        RemoveStrategy removeStrategy = null;
        checker = new BlackListStrategyRemoveChecker();
        if (!checker.isValid(blackListConfig.getRemoveStrategyConfigs())) {
            throw new SQLException("groupBalanceStrategy config incorrect："
                                   + blackListConfig.getRemoveStrategyConfigs());
        }
        switch (name) {
            case "TIMETOUT":
            default:
                removeStrategy = new TimeoutRecover();
                int timeout = Integer.parseInt(blackListConfig.getRemoveStrategyConfigs().get(
                    "TIMEOUT"));
                ((TimeoutRecover) removeStrategy).setTimeout(timeout);
                break;

        }
        blackListConfig.setRemoveStrategy(removeStrategy);
        name = blackListConfig.getAppendStrategyConfigs().get("NAME");
        checker = new BlackListStrategyAppendChecker();
        if (!checker.isValid(blackListConfig.getAppendStrategyConfigs())) {
            throw new SQLException("groupBalanceStrategy config incorrect："
                                   + blackListConfig.getAppendStrategyConfigs());
        }
        AppendStrategy appendStrategy = null;
        switch (name) {
            case "RETRYDURATION":
                appendStrategy = new RetryDuration();
                String retryTimes = blackListConfig.getAppendStrategyConfigs().get("RETRYTIMES");
                String duration = blackListConfig.getAppendStrategyConfigs().get("DURATION");
                ((RetryDuration) appendStrategy).setRetryTimes(Integer.parseInt(retryTimes));
                ((RetryDuration) appendStrategy).setDuration(Integer.parseInt(duration));
                break;
            case "NORMAL":
            default:
                appendStrategy = new NormalAppend();
                break;
        }
        blackListConfig.setAppendStrategy(appendStrategy);
    }

    @Override
    public void buildBalanceStrategy() throws SQLException {
        HashMap<String, String> map = loadBalanceInfo.getBalanceStrategyConfigs();
        BalanceStrategy balanceStrategy = null;
        String name = map.get("NAME");
        checker = new BalanceStrategyChecker();
        if (!checker.isValid(map)) {
            throw new SQLException("Global balanceStrategy config incorrect：" + map);
        }
        switch (name) {
            case "SERVERAFFINITY":
                balanceStrategy = new ServerAffinityStrategy();
                break;
            case "ROTATION":
                balanceStrategy = new RotationStrategy();
                break;
            case "RANDOM":
            default:
                balanceStrategy = new RandomStrategy();
                break;
        }
        loadBalanceInfo.setBalanceStrategy(balanceStrategy);
    }

    @Override
    public void buildHostListBalanceStrategies() throws SQLException {
        checker = new BalanceStrategyChecker();
        for (LoadBalanceAddressList loadBalanceAddressList : loadBalanceInfo.groups) {
            String name = loadBalanceAddressList.getBalanceStrategyConfigs().get("NAME");
            HashMap<String, String> map = loadBalanceAddressList.getBalanceStrategyConfigs();
            if (!checker.isValid(map)) {
                throw new SQLException("Host list  balanceStrategy config incorrect：" + map);
            }
            BalanceStrategy hostListBalanceStrategy = null;
            switch (name) {
                case "SERVERAFFINITY":
                    hostListBalanceStrategy = new ServerAffinityStrategy();
                    break;
                case "ROTATION":
                    hostListBalanceStrategy = new RotationStrategy();
                    break;
                case "RANDOM":
                default:
                    hostListBalanceStrategy = new RandomStrategy();
                    break;
            }
            loadBalanceAddressList.setBalanceStrategy(hostListBalanceStrategy);
        }
    }

    @Override
    public LoadBalanceDriver getResult() {
        return loadBalanceDriver;
    }

}
