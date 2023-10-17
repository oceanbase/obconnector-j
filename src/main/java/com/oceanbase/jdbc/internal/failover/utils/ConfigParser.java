package com.oceanbase.jdbc.internal.failover.utils;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.BlackList.append.RetryDuration;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceHostAddress;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceInfo;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.util.HttpClient;
import com.oceanbase.jdbc.internal.util.JsonParser;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.constant.HaMode;
import com.oceanbase.jdbc.util.Options;

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
    LOADBALANCE("LOAD_BALANCE"),
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

    private static final Logger logger = LoggerFactory.getLogger(ConfigParser.class);
    private static final ReentrantLock lock = new ReentrantLock();

    // LoadBalanceInfo represents the configuration information of the current net_service_name
    static ConcurrentHashMap<String, LoadBalanceInfo> loadBalanceInfos = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, Long>            ocpAccessTimes = new ConcurrentHashMap<>();

    // TNS file on disk, or extend description in URL
    public static void readLoadBalanceInfosFromTns(Reader reader) throws IOException {
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
                            loadBalanceInfos.put(currentNetServiceName,loadBalanceInfo);
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
                                    loadBalanceInfo.getGroupBalanceStrategyConfigs().put(Consts.NAME,value.toUpperCase());
                                    break;
                                case LOADBALANCESTRATEGY:
                                case LOADBALANCESTRATEGYALIAS:
                                    if(curZone == ZONE.INDESCRIPTION) {
                                        loadBalanceInfo.getBalanceStrategyConfigs().put(Consts.NAME,value.toUpperCase());
                                    } else {
                                        loadBalanceInfo.getGroups().get(loadBalanceInfo.getGroups().size() -1 ).getBalanceStrategyConfigs().put(Consts.NAME,value.toUpperCase());
                                    }
                                    break;
                                case LOADBALANCEALIAS:
                                case LOADBALANCE:
                                    if(value.equalsIgnoreCase(Consts.ON)) {
                                        loadBalanceInfo.setHaMode(HaMode.LOADBALANCE);
                                    } else {
                                        loadBalanceInfo.setHaMode(HaMode.NONE);
                                    }
                                    break;
                                case NAME:
                                    if(curZone == ZONE.INREMOVESTRATGY) {
                                        blackListConfig.getRemoveStrategyConfigs().put(Consts.NAME,Consts.TIMEOUT_MS);
                                    } else {
                                        blackListConfig.getAppendStrategyConfigs().put(Consts.NAME,value.toUpperCase());
                                    }
                                    break;
                                case TIMEOUT:
                                    blackListConfig.getRemoveStrategyConfigs().put(Consts.TIMEOUT_MS,value.toUpperCase());
                                    break;
                                case RETRYTIMES:
                                    blackListConfig.getAppendStrategyConfigs().put(Consts.RETRYTIMES,value.toUpperCase());
                                    break;
                                case DURATION:
                                    blackListConfig.getAppendStrategyConfigs().put(Consts.DURATION_MS,value.toUpperCase());
                                    RetryDuration.updateMaxDuration(Long.parseLong(value));
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
    }

    public static void readLoadBalanceInfosFromJsonFile() throws IOException, SQLException {
        logger.debug("Read json file on disk...");
        ConfigInfo jsonFileInfo = getJsonFilePath();
        String filePath = jsonFileInfo.path + "/" + jsonFileInfo.name;
        File file = new File(filePath);

        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        StringBuffer content = new StringBuffer();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        String jsonBalanceInfos = content.toString();

        if (!jsonBalanceInfos.equals("")) {
            JsonParser parsing = new JsonParser(jsonBalanceInfos);
            parsing.parse();
            ArrayList listServices = (ArrayList) ((HashMap) parsing.getStack().peek()).get("NET_SERVICES");
            for (HashMap mapService : (Iterable<HashMap>) listServices) {
                String netServiceName = (String) mapService.get("NET_SERVICE_NAME");
                HashMap mapDescription = (HashMap) mapService.get("DESCRIPTION");
                LoadBalanceInfo balanceInfo = new LoadBalanceInfo(mapDescription);
                loadBalanceInfos.put(netServiceName, balanceInfo); // add to cache
            }
        }
    }

    public static void writeLoadBalanceInfosIntoJsonFile() throws IOException, SQLException {
        logger.debug("Write json file on disk...");
        StringBuilder json = new StringBuilder("{\"NET_SERVICES\": [");

        boolean atLeastOneNet = false;
        Iterator<Map.Entry<String, LoadBalanceInfo>> iterator = loadBalanceInfos.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, LoadBalanceInfo> entry = iterator.next();
            String netServiceName = entry.getKey();
            LoadBalanceInfo balanceInfo = entry.getValue();

            if (atLeastOneNet) {
                json.append(",");
            } else {
                atLeastOneNet = true;
            }
            json.append("{");
            json.append("\"NET_SERVICE_NAME\":\"" + netServiceName + "\"");
            json.append(",\"DESCRIPTION\":{" + balanceInfo.toJson() + "}");
            json.append("}");
        }

        json.append("]}");

        ConfigInfo jsonFileInfo = getJsonFilePath();
        String filePath = jsonFileInfo.path + "/" + jsonFileInfo.name;
        File file = new File(filePath);

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
        out.write(json.toString());
        out.close();
    }

    public static LoadBalanceInfo getLoadBalanceInfoFromTns (UrlParser urlParser) throws SQLException, IOException {
        if (Utils.tnsDaemon == null) {
            ConfigInfo tnsFileInfo = getTnsFilePath();
            String filePath = tnsFileInfo.path + "/" + tnsFileInfo.name;
            File file = new File(filePath);
            Reader reader = new InputStreamReader(new FileInputStream(file));
            ConfigParser.readLoadBalanceInfosFromTns(reader);
            return loadBalanceInfos.get(urlParser.getTnsServiceName());
        } else {
            if (Utils.tnsDaemon.getState() == Thread.State.TERMINATED) {
                throw new SQLException("Config file daemon thread  is TERMINATED");
            }
            LoadBalanceInfo tnsPropertis = loadBalanceInfos.get(urlParser.getTnsServiceName());
            if (tnsPropertis == null) {
                throw new SQLException("Unknown host specified ");
            }
            return tnsPropertis;
        }
    }

    public static LoadBalanceInfo getLoadBalanceInfoFromExtendDescription (UrlParser urlParser) throws SQLException, IOException {
        try {
            ConfigParser.readLoadBalanceInfosFromTns(new StringReader(urlParser.getExtendDescription()));
        } catch (Exception e) {
            SQLException sqlException = new SQLException("LoadBalance config error", e);
            throw sqlException;
        }
        //extend description with net service name (NET_SERVICE_NAME=(DESCRIPTION=()))
        //extend description without net service name (DESCRIPTION=())
        String netServiceName = getNetServiceName(urlParser.getExtendDescription());
        return loadBalanceInfos.get(netServiceName);
    }

    public static LoadBalanceInfo getLoadBalanceInfoFromOcpApi(OcpApi ocpApi, Options options) throws IOException, SQLException {
        // get config from OCP
        String jsonBalanceInfo = null;
        boolean ocpAavailable = true;
        String ocpUrl = null;

        try {
            ocpAccessTimes.putIfAbsent(ocpApi.appName, 0L);
            if (System.currentTimeMillis() - ocpAccessTimes.get(ocpApi.appName) < options.ocpAccessInterval * 60 * 1000) {
                return ConfigParser.getGlobalLoadBalanceInfo(ocpApi.appName);
            }
            ocpUrl = "http://" + ocpApi.ip + ":" + ocpApi.port + "/api/v2/obproxy/loadBalanceInfo?appName=" + ocpApi.appName;

            Iterator<String> iter = ocpAccessTimes.keySet().iterator();
            String targetApp = null;
            while (iter.hasNext() && (targetApp = iter.next()).equals(ocpApi.appName)) {
            }
            logger.debug("Key : (hashCode={}, toString={}) --> Value : (hashCode={}, toString={})", targetApp.hashCode(), targetApp, ocpAccessTimes.get(targetApp).hashCode(), ocpAccessTimes.get(targetApp));

            synchronized (targetApp) {
                logger.debug("synchronized begin {}", ocpApi.appName);
                if (System.currentTimeMillis() - ocpAccessTimes.get(ocpApi.appName) < options.ocpAccessInterval * 60 * 1000) {
                    logger.debug("synchronized return {}", ocpApi.appName);
                    return ConfigParser.getGlobalLoadBalanceInfo(ocpApi.appName);
                }
                logger.debug("Accessing OCP API... " + ocpUrl);
                jsonBalanceInfo = HttpClient.doGet(ocpUrl, options);
                ocpAccessTimes.put(ocpApi.appName, System.currentTimeMillis());

                if (jsonBalanceInfo != null) {
                    logger.debug("Load balance info from OCP : " + jsonBalanceInfo);

                    JsonParser parsing = new JsonParser(jsonBalanceInfo);
                    parsing.parse();
                    HashMap mapService = (HashMap) ((HashMap) parsing.getStack().peek()).get("data");
                    String netServiceName = (String) mapService.get("NET_SERVICE_NAME");
                    HashMap mapDescription = (HashMap) mapService.get("DESCRIPTION");
                    LoadBalanceInfo balanceInfo = new LoadBalanceInfo(mapDescription);

                    // Refreshing the json file must apply for a lock
                    lock.lock();
                    try {
                        readLoadBalanceInfosFromJsonFile();
                    } catch (Exception e) {
                        // Reading files can fail, but writing files must be executed
                        logger.warn("Failed to read json file on disk");
                    }
                    logger.debug("loadBalanceInfos.put({}, balanceInfo)", ocpApi.appName);
                    loadBalanceInfos.put(netServiceName, balanceInfo); // add to cache
                    writeLoadBalanceInfosIntoJsonFile(); // save to file
                    lock.unlock();
                }

                logger.debug("synchronized end {}", ocpApi.appName);
            }
        } catch (Exception e) {
            logger.warn("OCP API isn't available, " + ocpUrl);
            ocpAavailable = false;
        }

        if (!ocpAavailable) {
            // get config from file
            readLoadBalanceInfosFromJsonFile();
        }

        return ConfigParser.getGlobalLoadBalanceInfo(ocpApi.appName);
    }

    public static LoadBalanceInfo getLoadBalanceInfoByDefault (UrlParser urlParser) {
        LoadBalanceInfo ret = new LoadBalanceInfo();
        String strategy = urlParser.getOptions().loadBalanceStrategy.toUpperCase();
        ret.getBalanceStrategyConfigs().put(Consts.NAME,strategy);
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
            LoadBalanceAddressList  loadBalanceAddressList = new LoadBalanceAddressList();
            loadBalanceAddressList.getBalanceStrategyConfigs().put(Consts.NAME,strategy);
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
        return ret;
    }

    public static LoadBalanceInfo getGlobalLoadBalanceInfo (String netServiceName) {
        return loadBalanceInfos.get(netServiceName);
    }

    public static String getNetServiceName(String extendDescription) throws SQLException {
        String tmp = extendDescription.trim();
        int startIndex = tmp.indexOf("(");
        int endIndex = tmp.indexOf("=");
        String value = tmp.substring(startIndex+1,endIndex);
        if(value.trim().equalsIgnoreCase("DESCRIPTION")) {
            throw new SQLException("Url config format error !");
        } else {
            return value.trim();
        }
    }

    public static class ConfigInfo {
        public String name;
        public String path;
    }

    /**
     * Get current config information
     * @return ConfigInfo include path and file name.
     * @throws SQLException
     */
    public static ConfigInfo getTnsFilePath() throws SQLException {
        ConfigInfo configInfo = new ConfigInfo();
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
        configInfo.path = tnsPath;
        configInfo.name = configFileName;
        return configInfo;
    }

    /**
     * Get current config information
     * @return ConfigInfo include path and file name.
     * @throws SQLException
     */
    public static ConfigInfo getJsonFilePath() throws SQLException {
        ConfigInfo configInfo = new ConfigInfo();

        // use the same file path with tns config
        String tnsPath = System.getProperty("oceanbase.tns_admin");
        String tnsEnv = System.getenv("OCEANBASE_TNS_ADMIN");
        if (tnsEnv == null && tnsPath == null) {
            throw new SQLException("Unknown TNS_ADMIN specified ");
        }
        if (tnsPath == null) {
            tnsPath = tnsEnv;
        }

        // use different file name from json config
        String configFileName = "jsonnames.ob";
        String jsonConfigName = System.getProperty("oceanbase.json_admin_name");
        String jsonConfigNameEnv = System.getenv("OCEANBASE_JSON_ADMIN_NAME");
        if (jsonConfigName != null) {
            configFileName = jsonConfigName;
        }
        if (jsonConfigNameEnv != null) {
            configFileName = jsonConfigNameEnv;
        }

        configInfo.path = tnsPath;
        configInfo.name = configFileName;
        return configInfo;
    }

    public static class OcpApi {
        public String ip;
        public String port;
        public String appName;
    }

}
