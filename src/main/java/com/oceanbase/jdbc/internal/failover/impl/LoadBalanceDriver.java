package com.oceanbase.jdbc.internal.failover.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.BlackList.append.AppendStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.append.NormalAppend;
import com.oceanbase.jdbc.internal.failover.BlackList.append.RetryDuration;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.TimeoutRecover;
import com.oceanbase.jdbc.internal.failover.Listener;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.*;
import com.oceanbase.jdbc.internal.failover.impl.checkers.*;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.failover.utils.Consts;
import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class LoadBalanceDriver {
    private LoadBalanceInfo loadBalanceInfo;
    ConfigChecker           checker;

    public LoadBalanceDriver(LoadBalanceInfo loadBalanceInfo) {
        this.loadBalanceInfo = loadBalanceInfo;
    }

    public void construct() throws SQLException {
        buildGroupStrategy();
        buildBlackListStrategy();
        buildBalanceStrategy();
        buildHostListBalanceStrategies();
    }

    public void buildGroupStrategy() throws SQLException {
        HashMap<String, String> map = loadBalanceInfo.getGroupBalanceStrategyConfigs();
        GroupBalanceStrategy groupBalanceStrategy = null;
        String name = map.get(Consts.NAME);
        checker = new GroupStrategyConfigChecker();
        if (!checker.isValid(map)) {
            throw new SQLException("groupBalanceStrategy config incorrect：" + map);
        }
        switch (name.toUpperCase()) {
            case Consts.ROTATION:
            default:
                groupBalanceStrategy = new GroupRotationStrategy();
                break;
        }
        loadBalanceInfo.setGroupBalanceStrategy(groupBalanceStrategy);
    }

    public void buildBlackListStrategy() throws SQLException {
        BlackListConfig blackListConfig = loadBalanceInfo.getBlackListConfig();
        String name = blackListConfig.getRemoveStrategyConfigs().get(Consts.NAME);
        RemoveStrategy removeStrategy = null;
        checker = new BlackListStrategyRemoveChecker();
        if (!checker.isValid(blackListConfig.getRemoveStrategyConfigs())) {
            throw new SQLException("groupBalanceStrategy config incorrect："
                                   + blackListConfig.getRemoveStrategyConfigs());
        }
        switch (name.toUpperCase()) {
            case Consts.TIMEOUT_MS:
            default:
                removeStrategy = new TimeoutRecover();
                int timeout = Integer.parseInt(blackListConfig.getRemoveStrategyConfigs().get(
                    Consts.TIMEOUT_MS));
                ((TimeoutRecover) removeStrategy).setTimeout(timeout);
                break;

        }
        blackListConfig.setRemoveStrategy(removeStrategy);
        name = blackListConfig.getAppendStrategyConfigs().get(Consts.NAME);
        checker = new BlackListStrategyAppendChecker();
        if (!checker.isValid(blackListConfig.getAppendStrategyConfigs())) {
            throw new SQLException("groupBalanceStrategy config incorrect："
                                   + blackListConfig.getAppendStrategyConfigs());
        }
        AppendStrategy appendStrategy = null;
        switch (name.toUpperCase()) {
            case Consts.RETRYDURATION:
                String retryTimes = blackListConfig.getAppendStrategyConfigs().get(
                    Consts.RETRYTIMES);
                String duration = blackListConfig.getAppendStrategyConfigs()
                    .get(Consts.DURATION_MS);
                appendStrategy = new RetryDuration(Long.parseLong(duration),
                    Integer.parseInt(retryTimes));
                break;
            case Consts.NORMAL:
            default:
                appendStrategy = new NormalAppend();
                break;
        }
        blackListConfig.setAppendStrategy(appendStrategy);
    }

    public void buildBalanceStrategy() throws SQLException {
        HashMap<String, String> map = loadBalanceInfo.getBalanceStrategyConfigs();
        BalanceStrategy balanceStrategy = null;
        String name = map.get(Consts.NAME);
        checker = new BalanceStrategyChecker();
        if (!checker.isValid(map)) {
            throw new SQLException("Global balanceStrategy config incorrect：" + map);
        }
        switch (name.toUpperCase()) {
            case Consts.SERVERAFFINITY:
                balanceStrategy = new ServerAffinityStrategy();
                break;
            case Consts.ROTATION:
                balanceStrategy = new RotationStrategy();
                break;
            case Consts.RANDOM:
            default:
                balanceStrategy = new RandomStrategy();
                break;
        }
        loadBalanceInfo.setBalanceStrategy(balanceStrategy);
    }

    public void buildHostListBalanceStrategies() throws SQLException {
        checker = new BalanceStrategyChecker();
        for (LoadBalanceAddressList loadBalanceAddressList : loadBalanceInfo.groups) {
            String name = loadBalanceAddressList.getBalanceStrategyConfigs().get(Consts.NAME);
            HashMap<String, String> map = loadBalanceAddressList.getBalanceStrategyConfigs();
            if (!checker.isValid(map)) {
                throw new SQLException("Host list  balanceStrategy config incorrect：" + map);
            }
            BalanceStrategy hostListBalanceStrategy = null;
            switch (name.toUpperCase()) {
                case Consts.SERVERAFFINITY:
                    hostListBalanceStrategy = new ServerAffinityStrategy();
                    break;
                case Consts.ROTATION:
                    hostListBalanceStrategy = new RotationStrategy();
                    break;
                case Consts.RANDOM:
                default:
                    hostListBalanceStrategy = new RandomStrategy();
                    break;
            }
            loadBalanceAddressList.setBalanceStrategy(hostListBalanceStrategy);
        }
    }

    public void loop(UrlParser urlParser, Listener listener, final GlobalStateInfo globalInfo,
                     SearchFilter searchFilter,
                     ConcurrentMap<HostAddress, HostStateInfo> blacklist,
                     Set<HostAddress> pickedList) throws SQLException {
        this.loadBalanceInfo.groupBalanceStrategy.pickAddressList(loadBalanceInfo.groups,
            urlParser, listener, globalInfo, searchFilter, blacklist, pickedList);
    }
}
