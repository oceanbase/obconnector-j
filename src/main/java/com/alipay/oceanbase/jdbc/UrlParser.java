package com.alipay.oceanbase.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.alipay.oceanbase.jdbc.credential.CredentialPlugin;
import com.alipay.oceanbase.jdbc.credential.CredentialPluginLoader;
import com.alipay.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import com.alipay.oceanbase.jdbc.internal.util.constant.ParameterConstant;
import com.alipay.oceanbase.jdbc.util.DefaultOptions;
import com.alipay.oceanbase.jdbc.util.Options;

/**
 * parse and verification of URL.
 *
 * <p>basic syntax :<br>
 * {@code
 * jdbc:oceanbase:[replication:|failover|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>]/[database>]
 * [?<key1>=<value1>[&<key2>=<value2>]] }
 *
 * <p>hostDescription:<br>
 * - simple :<br>
 * {@code <host>:<portnumber>}<br>
 * (for example localhost:3306)<br>
 * <br>
 * - complex :<br>
 * {@code address=[(type=(master|slave))][(port=<portnumber>)](host=<host>)}<br>
 * <br>
 * <br>
 * type is by default master<br>
 * port is by default 3306<br>
 *
 * <p>host can be dns name, ipv4 or ipv6.<br>
 * in case of ipv6 and simple host description, the ip must be written inside bracket.<br>
 * exemple : {@code jdbc:oceanbase://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 *
 * <p>Some examples :<br>
 * {@code jdbc:oceanbase://localhost:3306/database?user=greg&password=pass}<br>
 * {@code
 * jdbc:oceanbase://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}
 * <br>
 */
public class UrlParser implements Cloneable {

    private static final String  DISABLE_MYSQL_URL = "disableMariaDbDriver";
    private static final Pattern URL_PARAMETER     = Pattern.compile("(\\/([^\\?]*))?(\\?(.+))*",
                                                       Pattern.DOTALL);
    private static final Pattern AWS_PATTERN       = Pattern
                                                       .compile(
                                                           "(.+)\\.([a-z0-9\\-]+\\.rds\\.amazonaws\\.com)",
                                                           Pattern.CASE_INSENSITIVE);

    private String               database;
    private Options              options           = null;
    private List<HostAddress>    addresses;
    private HaMode               haMode;
    private String               initialUrl;
    private boolean              multiMaster;
    private CredentialPlugin     credentialPlugin;

    private UrlParser() {
    }

    /**
     * Constructor.
     *
     * @param database database
     * @param addresses list of hosts
     * @param options connection option
     * @param haMode High availability mode
     * @throws SQLException if credential plugin cannot be loaded
     */
    public UrlParser(String database, List<HostAddress> addresses, Options options, HaMode haMode)
                                                                                                  throws SQLException {
        this.options = options;
        this.database = database;
        this.addresses = addresses;
        this.haMode = haMode;
        if (haMode == HaMode.AURORA) {
            for (HostAddress hostAddress : addresses) {
                hostAddress.type = null;
            }
        } else {
            for (HostAddress hostAddress : addresses) {
                if (hostAddress.type == null) {
                    hostAddress.type = ParameterConstant.TYPE_MASTER;
                }
            }
        }
        this.credentialPlugin = CredentialPluginLoader.get(options.credentialType);
        DefaultOptions.postOptionProcess(options, credentialPlugin);
        setInitialUrl();
        loadMultiMasterValue();
    }

    /**
     * Tell if mariadb driver accept url string. (Correspond to interface
     * java.jdbc.Driver.acceptsURL() method)
     *
     * @param url url String
     * @return true if url string correspond.
     */
    public static boolean acceptsUrl(String url) {
        return (url != null)
               && (url.startsWith("jdbc:oceanbase:") && !url.contains(DISABLE_MYSQL_URL));
    }

    public static UrlParser parse(final String url) throws SQLException {
        return parse(url, new Properties());
    }

    /**
     * Parse url connection string with additional properties.
     *
     * @param url connection string
     * @param prop properties
     * @return UrlParser instance
     * @throws SQLException if parsing exception occur
     */
    public static UrlParser parse(final String url, Properties prop) throws SQLException {
        if (url != null && (url.startsWith("jdbc:oceanbase:") && !url.contains(DISABLE_MYSQL_URL))) {
            UrlParser urlParser = new UrlParser();
            parseInternal(urlParser, url, (prop == null) ? new Properties() : prop);
            return urlParser;
        }
        return null;
    }

    /**
     * Parses the connection URL in order to set the UrlParser instance with all the information
     * provided through the URL.
     *
     * @param urlParser object instance in which all data from the connection url is stored
     * @param url connection URL
     * @param properties properties
     * @throws SQLException if format is incorrect
     */
    private static void parseInternal(UrlParser urlParser, String url, Properties properties)
                                                                                             throws SQLException {
        try {
            urlParser.initialUrl = url;
            int separator = url.indexOf("//");
            if (separator == -1) {
                throw new IllegalArgumentException(
                    "url parsing error : '//' is not present in the url " + url);
            }

            urlParser.haMode = parseHaMode(url, separator);

            String urlSecondPart = url.substring(separator + 2);
            int dbIndex = urlSecondPart.indexOf("/");
            int paramIndex = urlSecondPart.indexOf("?");

            String hostAddressesString;
            String additionalParameters;
            if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
                hostAddressesString = urlSecondPart.substring(0, paramIndex);
                additionalParameters = urlSecondPart.substring(paramIndex);
            } else if ((dbIndex < paramIndex && dbIndex > -1)
                       || (dbIndex > paramIndex && paramIndex < 0)) {
                hostAddressesString = urlSecondPart.substring(0, dbIndex);
                additionalParameters = urlSecondPart.substring(dbIndex);
            } else {
                hostAddressesString = urlSecondPart;
                additionalParameters = null;
            }

            defineUrlParserParameters(urlParser, properties, hostAddressesString,
                additionalParameters);
            setDefaultHostAddressType(urlParser);
            urlParser.loadMultiMasterValue();
        } catch (IllegalArgumentException i) {
            throw new SQLException("error parsing url : " + i.getMessage(), i);
        }
    }

    /**
     * Sets the parameters of the UrlParser instance: addresses, database and options. It parses
     * through the additional parameters given in order to extract the database and the options for
     * the connection.
     *
     * @param urlParser object instance in which all data from the connection URL is stored
     * @param properties properties
     * @param hostAddressesString string that holds all the host addresses
     * @param additionalParameters string that holds all parameters defined for the connection
     * @throws SQLException if credential plugin cannot be loaded
     */
    private static void defineUrlParserParameters(UrlParser urlParser, Properties properties,
                                                  String hostAddressesString,
                                                  String additionalParameters) throws SQLException {

        if (additionalParameters != null) {
            //noinspection Annotator
            Matcher matcher = URL_PARAMETER.matcher(additionalParameters);
            matcher.find();
            urlParser.database = matcher.group(2);
            urlParser.options = DefaultOptions.parse(urlParser.haMode, matcher.group(4),
                properties, urlParser.options);
            if (urlParser.database != null && urlParser.database.isEmpty()) {
                urlParser.database = null;
            }
        } else {
            urlParser.database = null;
            urlParser.options = DefaultOptions.parse(urlParser.haMode, "", properties,
                urlParser.options);
        }
        urlParser.credentialPlugin = CredentialPluginLoader.get(urlParser.options.credentialType);
        DefaultOptions.postOptionProcess(urlParser.options, urlParser.credentialPlugin);

        LoggerFactory.init(urlParser.options.log || urlParser.options.profileSql
                           || urlParser.options.slowQueryThresholdNanos != null);
        urlParser.addresses = HostAddress.parse(hostAddressesString, urlParser.haMode);
    }

    private static HaMode parseHaMode(String url, int separator) {
        // parser is sure to have at least 2 colon, since jdbc:oceanbase: is tested.
        int firstColonPos = url.indexOf(':');
        int secondColonPos = url.indexOf(':', firstColonPos + 1);
        int thirdColonPos = url.indexOf(':', secondColonPos + 1);
        int forthColonPos = url.indexOf(':', thirdColonPos + 1);

        if (thirdColonPos > separator || thirdColonPos == -1) {
            if (secondColonPos == separator - 1) {
                return HaMode.NONE;
            }
            thirdColonPos = separator;
        }

        if (forthColonPos > separator || forthColonPos == -1) {
            forthColonPos = separator;
        }

        try {
            String haModeString = url.substring(secondColonPos + 1, thirdColonPos).toUpperCase(
                Locale.ROOT);
            if ("ORACLE".equals(haModeString)) {
                haModeString = url.substring(thirdColonPos + 1, forthColonPos).toUpperCase(
                    Locale.ROOT);
            }
            if ("".equals(haModeString)) {
                return HaMode.NONE;
            } else if ("FAILOVER".equals(haModeString)) {
                haModeString = "LOADBALANCE";
            }
            return HaMode.valueOf(haModeString);
        } catch (IllegalArgumentException i) {
            throw new IllegalArgumentException(
                "wrong failover parameter format in connection String " + url);
        }
    }

    private static void setDefaultHostAddressType(UrlParser urlParser) {
        if (urlParser.haMode == HaMode.AURORA) {
            for (HostAddress hostAddress : urlParser.addresses) {
                hostAddress.type = null;
            }
        } else {
            for (HostAddress hostAddress : urlParser.addresses) {
                if (hostAddress.type == null) {
                    hostAddress.type = ParameterConstant.TYPE_MASTER;
                }
            }
        }
    }

    private void setInitialUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:oceanbase:");
        if (haMode != HaMode.NONE) {
            sb.append(haMode.toString().toLowerCase(Locale.ROOT)).append(":");
        }
        sb.append("//");

        for (int i = 0; i < addresses.size(); i++) {
            HostAddress hostAddress = addresses.get(i);
            if (i > 0) {
                sb.append(",");
            }
            sb.append("address=(host=").append(hostAddress.host).append(")").append("(port=")
                .append(hostAddress.port).append(")");
            if (hostAddress.type != null) {
                sb.append("(type=").append(hostAddress.type).append(")");
            }
        }

        sb.append("/");
        if (database != null) {
            sb.append(database);
        }
        DefaultOptions.propertyString(options, haMode, sb);
        initialUrl = sb.toString();
    }

    /**
     * Permit to set parameters not forced. if options useBatchMultiSend and usePipelineAuth are not
     * explicitly set in connection string, value will default to true or false according if aurora
     * detection.
     *
     * @return UrlParser for easy testing
     */
    public UrlParser auroraPipelineQuirks() {

        // Aurora has issue with pipelining, depending on network speed.
        // Driver must rely on information provided by user : hostname if dns, and HA mode.</p>
        boolean disablePipeline = isAurora();

        if (options.useBatchMultiSend == null) {
            options.useBatchMultiSend = disablePipeline ? Boolean.FALSE : Boolean.TRUE;
        }

        if (options.usePipelineAuth == null) {
            options.usePipelineAuth = disablePipeline ? Boolean.FALSE : Boolean.TRUE;
        }
        return this;
    }

    /**
     * Detection of Aurora.
     *
     * <p>Aurora rely on MySQL, then cannot be identified by protocol. But Aurora doesn't permit some
     * behaviour normally working with MySQL : pipelining. So Driver must identified if server is
     * Aurora to disable pipeline options that are enable by default.
     *
     * @return true if aurora.
     */
    public boolean isAurora() {
        if (haMode == HaMode.AURORA) {
            return true;
        }
        if (addresses != null) {
            for (HostAddress hostAddress : addresses) {
                Matcher matcher = AWS_PATTERN.matcher(hostAddress.host);
                if (matcher.find()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Parse url connection string.
     *
     * @param url connection string
     * @throws SQLException if url format is incorrect
     */
    public void parseUrl(String url) throws SQLException {
        if (acceptsUrl(url)) {
            parseInternal(this, url, new Properties());
        }
    }

    public String getUsername() {
        return options.user;
    }

    public void setUsername(String username) {
        options.user = username;
    }

    public String getPassword() {
        return options.password;
    }

    public void setPassword(String password) {
        options.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<HostAddress> getHostAddresses() {
        return this.addresses;
    }

    public Options getOptions() {
        return options;
    }

    protected void setProperties(String urlParameters) {
        DefaultOptions.parse(this.haMode, urlParameters, this.options);
        setInitialUrl();
    }

    public CredentialPlugin getCredentialPlugin() {
        return credentialPlugin;
    }

    /**
     * ToString implementation.
     *
     * @return String value
     */
    public String toString() {
        return initialUrl;
    }

    public String getInitialUrl() {
        return initialUrl;
    }

    public HaMode getHaMode() {
        return haMode;
    }

    @Override
    public boolean equals(Object parser) {
        if (this == parser) {
            return true;
        }
        if (!(parser instanceof UrlParser)) {
            return false;
        }

        UrlParser urlParser = (UrlParser) parser;
        return (initialUrl != null ? initialUrl.equals(urlParser.getInitialUrl()) : urlParser
            .getInitialUrl() == null)
               && (getUsername() != null ? getUsername().equals(urlParser.getUsername())
                   : urlParser.getUsername() == null)
               && (getPassword() != null ? getPassword().equals(urlParser.getPassword())
                   : urlParser.getPassword() == null);
    }

    @Override
    public int hashCode() {
        int result = options.password != null ? options.password.hashCode() : 0;
        result = 31 * result + (options.user != null ? options.user.hashCode() : 0);
        result = 31 * result + initialUrl.hashCode();
        return result;
    }

    private void loadMultiMasterValue() {
        if (haMode == HaMode.SEQUENTIAL || haMode == HaMode.REPLICATION
            || haMode == HaMode.LOADBALANCE) {
            boolean firstMaster = false;
            for (HostAddress host : addresses) {
                if (host.type.equals(ParameterConstant.TYPE_MASTER)) {
                    if (firstMaster) {
                        multiMaster = true;
                        return;
                    } else {
                        firstMaster = true;
                    }
                }
            }
        }
        multiMaster = false;
    }

    public boolean isMultiMaster() {
        return multiMaster;
    }

    @Override
  public Object clone() throws CloneNotSupportedException {
    UrlParser tmpUrlParser = (UrlParser) super.clone();
    tmpUrlParser.options = (Options) options.clone();
    tmpUrlParser.addresses = new ArrayList<>();
    tmpUrlParser.addresses.addAll(addresses);
    return tmpUrlParser;
  }
}
