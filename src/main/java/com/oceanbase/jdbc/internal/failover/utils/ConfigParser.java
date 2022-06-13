package com.oceanbase.jdbc.internal.failover.utils;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceHostAddress;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceInfo;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.constant.HaMode;

enum STATE {
    NONE,
    INBRACKETS,
    LEAFBRACKETS,
    RIGHTBRACKETS,
    STRING
}

enum ZONE {
    NONE,
    INDESCRIPTION,
    INBLACKLIST,
    INADDRESSLIST,
    INREMOVESTRATGY,
    INAPPENDSTRATGY,
    INADDRESS
}

enum CONFIGTYPE {
    HOST("HOST"),
    PORT("PORT"),
    WEIGHT("WEIGHT"),
    PROTOCOL("PROTOCOL"),
    DESCRIPTION("DESCRIPTION"),
    ADDRESSLIST("ADDRESS_LIST"),
    ADDRESS("ADDRESS"),
    LOADBALANCE("LOADBALANCE"),
    LOADBALANCEALIAS("OBLB"),
    LOADBALANCESTRATEGY("LOAD_BALANCE_STRATEGY"),
    LOADBALANCESTRATEGYALIAS("OBLB_STRATEGY"),
    SERVICENAME("SERVICE_NAME"),
    CONNECTDATA("CONNECT_DATA"),
    RETRYALLDOWNS("OBLB_RETRY_ALL_DOWNS"),
    GROUPSTRATEGY("OBLB_GROUP_STRATEGY"),
    BLACKLISTSTRATECONFIG("OBLB_BLACKLIST"),
    REMOVESTRATEGY("REMOVE_STRATEGY"),
    APPENDSTRATEGY("APPEND_STRATEGY"),
    TIMEOUT("TIMEOUT"),
    RETRYTIMES("RETRYTIMES"),
    DURATION("DURATION"),
    NAME("NAME"),
    SERVICE_NAME("SERVICE_NAME"),
    NONE("");
    public String name = null;

    CONFIGTYPE(String name) {
        this.name = name;
    }
}

public class ConfigParser {

    public static HashMap<String,LoadBalanceInfo> getLoadBalanceInfosFromReader(Reader reader) throws IOException {
        HashMap<String,LoadBalanceInfo>  balanceInfos = new HashMap<>();
        LoadBalanceInfo loadBalanceInfo = null;
        BlackListConfig blackListConfig = null;
        LoadBalanceAddressList loadBalanceAddressList = null;
        LoadBalanceHostAddress loadBalanceHostAddress = null;
        StringBuilder word = new StringBuilder();
        char buffer[] = new char[1024];
        int  readNum = 0;
        STATE state = STATE.NONE;
        int bracketsCount = 0;
        char cur;
        CONFIGTYPE curConfigType = CONFIGTYPE.NONE;
        String str = null ;
        ZONE curZone =  ZONE.NONE;
        String currentNetServiceName = "";
        while((readNum = reader.read(buffer)) != -1){
            for(int i = 0; i< readNum ;i++) {
                cur = buffer[i];
                switch (cur) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        // skip
                        continue;
                    case '(':
                        state =  STATE.LEAFBRACKETS;
                        bracketsCount ++;
                        break;
                    case ')':
                        bracketsCount --;
                        if(bracketsCount == 0) {
                            balanceInfos.put(currentNetServiceName,loadBalanceInfo);
                            state =  STATE.NONE;
                            curConfigType = CONFIGTYPE.NONE;
                            word = new StringBuilder();
                        }
                        if(bracketsCount > 0  && curConfigType != CONFIGTYPE.NONE) {
                            String value = word.toString();
                            switch (curConfigType) {
                                case SERVICE_NAME:
                                    loadBalanceInfo.setServiceName(value);
                                    break;
                                case RETRYALLDOWNS:
                                    loadBalanceInfo.setRetryAllDowns(Integer.parseInt(value));
                                    break;
                                case GROUPSTRATEGY:
                                    loadBalanceInfo.getGroupBalanceStrategyConfigs().put("NAME",value);
                                    break;
                                case LOADBALANCESTRATEGY:
                                case LOADBALANCESTRATEGYALIAS:
                                    if(curZone == ZONE.INDESCRIPTION) {
                                        loadBalanceInfo.getBalanceStrategyConfigs().put("NAME",value);
                                    } else {
                                        loadBalanceInfo.getGroups().get(loadBalanceInfo.getGroups().size() -1 ).getBalanceStrategyConfigs().put("NAME",value);
                                    }
                                case LOADBALANCEALIAS:
                                case LOADBALANCE:
                                    if(value.equalsIgnoreCase("ON")) {
                                        loadBalanceInfo.setHaMode(HaMode.LOADBALANCE);
                                    } else {
                                        loadBalanceInfo.setHaMode(HaMode.NONE);
                                    }
                                    break;
                                case NAME:
                                    if(curZone == ZONE.INREMOVESTRATGY) {
                                        blackListConfig.getRemoveStrategyConfigs().put("NAME","TIMEOUT");
                                    } else {
                                        blackListConfig.getAppendStrategyConfigs().put("NAME",value.toUpperCase());
                                    }
                                    break;
                                case TIMEOUT:
                                    blackListConfig.getRemoveStrategyConfigs().put("TIMEOUT",value);
                                    break;
                                case RETRYTIMES:
                                    blackListConfig.getAppendStrategyConfigs().put("RETRYTIMES",value);
                                    break;
                                case DURATION:
                                    blackListConfig.getAppendStrategyConfigs().put("DURATION",value);
                                    break;
                                case PORT:
                                    loadBalanceHostAddress.setPort(Integer.parseInt(value));
                                    break;
                                case HOST:
                                    loadBalanceHostAddress.setHost(value);
                                    break;
                                case WEIGHT:
                                    loadBalanceHostAddress.setWeight(Integer.parseInt(value));
                                    break;
                                default:
                                    break;
                            }
                            state =  STATE.NONE;
                            curConfigType = CONFIGTYPE.NONE;
                            word = new StringBuilder();
                            // skip
                            continue;
                        }
                        if(state ==  STATE.INBRACKETS) {
                            str = word.toString();
                            if(bracketsCount == 0) {
                                state =  STATE.NONE;
                                // skip
                                continue;
                            } else {
                                state =  STATE.RIGHTBRACKETS;
                            }
                            // reset the string builder
                            word = new StringBuilder();
                        }

                        break;
                    case '=':
                        // reset type according to the key name
                        str = word.toString();
                        boolean inKeys = false;
                        CONFIGTYPE configEnums[] = CONFIGTYPE.values();
                        CONFIGTYPE tmp = CONFIGTYPE.NONE;
                        for(CONFIGTYPE v : configEnums) {
                            if (str.equalsIgnoreCase(v.name)) {
                                tmp = v;
                                inKeys = true;
                            }
                        }

                        if(!inKeys) {
                            currentNetServiceName = str;
                        }
                        if(tmp != CONFIGTYPE.NONE) {
                            curConfigType = tmp;
                        }
                        switch (curConfigType) {
                            case DESCRIPTION:
                                curZone = ZONE.INDESCRIPTION;
                                loadBalanceInfo = new LoadBalanceInfo();
                                break;
                            case BLACKLISTSTRATECONFIG:
                                curZone = ZONE.INBLACKLIST;
                                blackListConfig = new BlackListConfig();
                                loadBalanceInfo.setBlackListConfig(blackListConfig);
                                break;
                            case REMOVESTRATEGY:
                                curZone = ZONE.INREMOVESTRATGY;
                                break;
                            case APPENDSTRATEGY:
                                curZone = ZONE.INAPPENDSTRATGY;
                                break;
                            case ADDRESSLIST:
                                curZone = ZONE.INADDRESSLIST;
                                loadBalanceAddressList = new LoadBalanceAddressList();
                                loadBalanceInfo.getGroups().add(loadBalanceAddressList);
                                break;
                            case ADDRESS:
                                curZone = ZONE.INADDRESS;
                                loadBalanceHostAddress = new LoadBalanceHostAddress(null,0);
                                loadBalanceAddressList.getAddressList().add(loadBalanceHostAddress);
                                break;
                        }

                        // reset the string builder
                        word = new StringBuilder();
                        break;
                    default:
                        if(state ==  STATE.LEAFBRACKETS) {
                            state =  STATE.INBRACKETS;
                        }
                        word.append(cur);
                        break;
                }
            }
        }
        return balanceInfos;
    }

    public static class TnsFileInfo {
        public String name;
        public String path;
    }

    /**
     * Get current config information
     * @return TnsFileInfo include path and file name.
     * @throws SQLException
     */
    public static TnsFileInfo getTnsFilePath() throws SQLException {
        TnsFileInfo tnsFileInfo = new TnsFileInfo();
        String tnsPath = System.getProperty("oceanbase.tns_admin");
        String tnsEnv = System.getenv("OCEANBASE_TNS_ADMIN");
        if (tnsEnv == null && tnsPath == null) {
            throw new SQLException("Unknown TNS_ADMIN specified ");
        }
        if (tnsPath == null) {
            tnsPath = tnsEnv;
        }
        String configFileName = "tnsnames.ob";
        String tnsConfigName = System.getProperty("oceanbase.tns_admin_name");
        String tnsConfigNameEnv = System.getenv("OCEANBASE_TNS_ADMIN_NAME");
        if (tnsConfigName != null) {
            configFileName = tnsConfigName;
        }
        if (tnsConfigNameEnv != null) {
            configFileName = tnsConfigNameEnv;
        }
        tnsFileInfo.path = tnsPath;
        tnsFileInfo.name = configFileName;
        return tnsFileInfo;
    }

    /**
     * Obtain the current LoadBalance configuration information in different ways, add other
     * mechanisms here
     * @param urlParser oceanbase url parser ,get options from here
     * @return the loadbalance config information
     */
    public static LoadBalanceInfo getGloabalLoadBalanceInfo(UrlParser urlParser) throws SQLException, IOException {
        if (urlParser.getTnsServiceName() != null) {
            if(Utils.tnsDaemon == null) {
                TnsFileInfo tnsFileInfo = getTnsFilePath();
                String filePath = tnsFileInfo.path + "/" + tnsFileInfo.name;
                File file = new File(filePath);
                Reader reader = new InputStreamReader(new FileInputStream(file));
                HashMap<String, LoadBalanceInfo> tmp = ConfigParser.getLoadBalanceInfosFromReader(reader);
                return tmp.get(urlParser.getTnsServiceName());
            } else {
                if (Utils.tnsDaemon.getState() == Thread.State.TERMINATED) {
                    throw new SQLException("Config file daemon thread  is TERMINATED");
                }
                LoadBalanceInfo tnsPropertis = Utils.tnsDaemon.getTnsProperties(urlParser.getTnsServiceName());
                if (tnsPropertis == null) {
                    throw new SQLException("Unknown host specified ");
                }
                return tnsPropertis;
            }
        } else {
            if(urlParser.getExtendDescription() != null) {
                HashMap<String,LoadBalanceInfo>  map = new HashMap<>();
                map = ConfigParser.getLoadBalanceInfosFromReader(new StringReader(urlParser.getExtendDescription()));
                return map.get("");
            } else {
                LoadBalanceInfo ret = new LoadBalanceInfo();
                String strategy = urlParser.getOptions().loadBalanceStrategy;
                ret.getBalanceStrategyConfigs().put("NAME",strategy);
                String serverAffinityOrder = urlParser.getOptions().serverAffinityOrder;
                if (serverAffinityOrder == null || serverAffinityOrder.equals("")) {
                    List<HostAddress> addressList = urlParser.getHostAddresses();
                    LoadBalanceAddressList  loadBalanceAddressList = new LoadBalanceAddressList();
                    for(HostAddress address : addressList) {
                        LoadBalanceHostAddress loadBalanceHostAddress = new LoadBalanceHostAddress(address.host, address.port);
                        loadBalanceAddressList.addressList.add(loadBalanceHostAddress);
                    }
                    ret.getGroups().add(loadBalanceAddressList);

                } else {
                    serverAffinityOrder = serverAffinityOrder.trim();
                    String[] urls = serverAffinityOrder.split(",");
                    ConcurrentHashMap<HostAddress, Integer> weights = new ConcurrentHashMap<>();
                    LoadBalanceAddressList  loadBalanceAddressList = new LoadBalanceAddressList();
                    for (String str : urls) {
                        LoadBalanceHostAddress loadBalanceHostAddress = new LoadBalanceHostAddress("0.0.0.0",0);
                        String[] vals = str.split(":");
                        loadBalanceHostAddress.setHost(vals[0]);
                        loadBalanceHostAddress.setPort(Integer.parseInt(vals[1]));
                        if (vals.length == 3) {
                            loadBalanceHostAddress.setWeight(Integer.parseInt(vals[2]));
                        } else {
                            loadBalanceHostAddress.setWeight(1);
                        }
                        loadBalanceAddressList.addressList.add(loadBalanceHostAddress);
                    }
                    ret.getGroups().add(loadBalanceAddressList);
                }
                return  ret;
            }
        } // we can add othrer mode here
    }
}
