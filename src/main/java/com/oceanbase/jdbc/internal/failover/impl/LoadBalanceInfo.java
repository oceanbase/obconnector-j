package com.oceanbase.jdbc.internal.failover.impl;

import java.io.InvalidObjectException;
import java.util.*;

import com.oceanbase.jdbc.internal.failover.BlackList.*;
import com.oceanbase.jdbc.internal.failover.BlackList.append.AppendStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.append.NormalAppend;
import com.oceanbase.jdbc.internal.failover.BlackList.append.RetryDuration;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.TimeoutRecover;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.*;
import com.oceanbase.jdbc.internal.failover.utils.Consts;
import com.oceanbase.jdbc.internal.util.constant.HaMode;

public class LoadBalanceInfo {

    HaMode                       haMode;
    String                       serviceName;
    int                          retryAllDowns;
    GroupBalanceStrategy         groupBalanceStrategy;
    HashMap<String, String>      groupBalanceStrategyConfigs;
    BalanceStrategy              balanceStrategy;            // Global Load Balancing Strategy
    HashMap<String, String>      balanceStrategyConfigs;
    BlackListConfig              blackListConfig;            //  Global Load Balancing Black List Config Information
    List<LoadBalanceAddressList> groups;

    public LoadBalanceInfo() {
        groups  = new ArrayList<>();
        blackListConfig = new BlackListConfig();
        groupBalanceStrategyConfigs = new HashMap<>();
        groupBalanceStrategyConfigs.put(Consts.NAME,Consts.ROTATION);
        balanceStrategyConfigs = new HashMap<>();
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

    public LoadBalanceInfo(HashMap map) throws InvalidObjectException {
        // set serviceName
        HashMap connectMap = (HashMap) map.get("CONNECT_DATA");
        if (connectMap != null) {
            serviceName = (String) connectMap.get("SERVICE_NAME");
        }

        // set retryAllDowns
        try {
            retryAllDowns = (Integer) map.get("OBLB_RETRY_ALL_DOWNS");
        } catch (Exception e) {
            retryAllDowns = 120;
        }

        // set groupBalanceStrategy
        groupBalanceStrategy = resolveGroupBalanceStrategy((String) map.get("OBLB_GROUP_STRATEGY"));

        // set global balanceStrategy
        balanceStrategy = resolveBalanceStrategy((String) map.get("OBLB_STRATEGY"), true);

        // set global blackListConfig
        blackListConfig = resolveBlackListConfig((HashMap) map.get("OBLB_BLACKLIST"));

        // set groups
        groups = new ArrayList<>();
        ArrayList groupList = (ArrayList) map.get("ADDRESS_LIST");
        // ADDRESS_LIST can't be null
        if (groupList == null || groupList.isEmpty()) {
            throw new InvalidObjectException("ADDRESS_LIST can't be empty!");
        }
        for (HashMap groupMap : (Iterable<HashMap>) groupList) {
            // set local balanceStrategy if exists
            BalanceStrategy balanceStrategy = resolveBalanceStrategy((String) groupMap.get("OBLB_STRATEGY"), false);

            // set local blackListConfig if exists, not supported now
            //BlackListConfig blackListConfig = resolveBlackListConfig((HashMap) groupMap.get("OBLB_BLACKLIST"), false);

            // set addressList
            List<LoadBalanceHostAddress> balanceAddressList = resolveAddressList((ArrayList) groupMap.get("ADDRESS"), balanceStrategy);

            LoadBalanceAddressList group = new LoadBalanceAddressList(balanceStrategy, null, balanceAddressList);
            groups.add(group);
        }
    }

    @Override
    public String toString() {
        return "LoadBalanceInfo{" + "haMode=" + haMode + ", serviceName='" + serviceName + '\''
               + ", groupBalanceStrategy=" + groupBalanceStrategy + ", balanceStrategy="
               + balanceStrategy + ", blackListConfig=" + blackListConfig + ", retryAllDowns="
               + retryAllDowns + ", groups=" + groups + '}';
    }

    public String toJson() {
        StringBuilder json = new StringBuilder("\"OBLB\":\"ON\",\"OBLB_RETRY_ALL_DOWNS\":"
                                               + retryAllDowns);

        if (serviceName != null) {
            json.append(",\"CONNECT_DATA\":{\"SERVICE_NAME\":\"").append(serviceName).append("\"}");
        }
        if (groupBalanceStrategy != null) {
            json.append(",").append(groupBalanceStrategy.toJson());
        }
        if (balanceStrategy != null) {
            json.append(",").append(balanceStrategy.toJson());
        }
        if (blackListConfig != null) {
            json.append(",").append(blackListConfig.toJson());
        }
        if (groups != null && groups.size() > 0) {
            json.append(",").append("\"ADDRESS_LIST\": [");

            boolean atLeastOneGroup = false;
            for (LoadBalanceAddressList group : groups) {
                if (atLeastOneGroup) {
                    json.append(",");
                } else {
                    atLeastOneGroup = true;
                }
                json.append("{").append(group.toJson()).append("}");
            }

            json.append("]");
        }

        return json.toString();
    }

    private GroupBalanceStrategy resolveGroupBalanceStrategy(String strVal) {
        GroupBalanceStrategy groupBalanceStrategy;

        if (strVal != null) {
            switch (strVal) {
                case "ROTATION":
                default:
                    groupBalanceStrategy = new GroupRotationStrategy();
                    break;
            }
        } else {
            groupBalanceStrategy = new GroupRotationStrategy();
        }

        return groupBalanceStrategy;
    }

    private BalanceStrategy resolveBalanceStrategy(String strVal, boolean isGlobal) {
        BalanceStrategy balanceStrategy;

        if (strVal != null) {
            switch (strVal) {
                case "ROTATION":
                    balanceStrategy = new RotationStrategy();
                    break;
                case "SERVERAFFINITY":
                    balanceStrategy = new ServerAffinityStrategy();
                    break;
                case "RANDOM":
                default:
                    balanceStrategy = new RandomStrategy();
                    break;
            }
        } else if (isGlobal) {
            balanceStrategy = new RandomStrategy();
        } else {
            balanceStrategy = this.balanceStrategy;
        }

        return balanceStrategy;
    }

    private BlackListConfig resolveBlackListConfig(HashMap blacklistMap) {
        BlackListConfig blackListConfig = new BlackListConfig(true);

        if (blacklistMap != null) {
            HashMap removeMap = (HashMap) blacklistMap.get("REMOVE_STRATEGY");
            if (removeMap != null) {
                RemoveStrategy removeStrategy;

                String name = (String) removeMap.get("NAME");
                // if REMOVE_STRATEGY isn't null, then NAME can't be null
                switch (name) {
                    case "TIMEOUT":
                    default:
                        long timeout = ((Integer) removeMap.get("TIMEOUT")).longValue();
                        removeStrategy = new TimeoutRecover(timeout);
                        break;
                }
                blackListConfig.setRemoveStrategy(removeStrategy);
            }

            HashMap appendMap = (HashMap) blacklistMap.get("APPEND_STRATEGY");
            if (appendMap != null) {
                AppendStrategy appendStrategy;

                String name = (String) appendMap.get("NAME");
                // if APPEND_STRATEGY isn't null, then NAME can't be null
                switch (name) {
                    case "RETRYDURATION":
                        int retryTimes = (Integer) appendMap.get("RETRYTIMES");
                        long duration = ((Integer) appendMap.get("DURATION")).longValue();
                        appendStrategy = new RetryDuration(duration, retryTimes);
                        break;
                    case "NORMAL":
                    default:
                        appendStrategy = new NormalAppend();
                        break;
                }
                blackListConfig.setAppendStrategy(appendStrategy);
            }
        }

        return blackListConfig;
    }

    private List<LoadBalanceHostAddress> resolveAddressList(ArrayList addressList, BalanceStrategy balanceStrategy) throws InvalidObjectException {
        if (addressList == null || addressList.isEmpty()) {
            throw new InvalidObjectException("ADDRESS can't be empty!");
        }

        List<LoadBalanceHostAddress> balanceAddressList = null;

        for (HashMap addressMap : (Iterable<HashMap>) addressList) {
            String host = (String) addressMap.get("HOST");
            int port = (Integer) addressMap.get("PORT");
            LoadBalanceHostAddress balanceAddress = new LoadBalanceHostAddress(host, port);

            if (balanceStrategy instanceof ServerAffinityStrategy) {
                int weight = (Integer) addressMap.get("WEIGHT");
                balanceAddress.setWeight(weight);
            }

            if (balanceAddressList == null) {
                balanceAddressList = new ArrayList<>();
            }
            balanceAddressList.add(balanceAddress);
        }

        return balanceAddressList;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
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

    public void setGroupBalanceStrategy(GroupBalanceStrategy groupBalanceStrategy) {
        this.groupBalanceStrategy = groupBalanceStrategy;
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

    public int getRetryAllDowns() {
        return retryAllDowns;
    }

    public void setRetryAllDowns(int retryAllDowns) {
        this.retryAllDowns = retryAllDowns;
    }

    public List<LoadBalanceAddressList> getGroups() {
        return groups;
    }

    public void setGroups(List<LoadBalanceAddressList> groups) {
        this.groups = groups;
    }

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

}
