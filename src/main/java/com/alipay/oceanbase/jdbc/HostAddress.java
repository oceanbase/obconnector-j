package com.alipay.oceanbase.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.alipay.oceanbase.jdbc.internal.logging.Logger;
import com.alipay.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import com.alipay.oceanbase.jdbc.internal.util.constant.ParameterConstant;

public class HostAddress {

    private static final Logger logger = LoggerFactory.getLogger(HostAddress.class);

    public String               host;
    public int                  port;
    public String               type   = null;

    private HostAddress() {
    }

    /**
     * Constructor. type is master.
     *
     * @param host host
     * @param port port
     */
    public HostAddress(String host, int port) {
        this.host = host;
        this.port = port;
        this.type = ParameterConstant.TYPE_MASTER;
    }

    /**
     * Constructor.
     *
     * @param host host
     * @param port port
     * @param type type
     */
    public HostAddress(String host, int port, String type) {
        this.host = host;
        this.port = port;
        this.type = type;
    }

    /**
     * parse - parse server addresses from the URL fragment.
     *
     * @param spec list of endpoints in one of the forms 1 - host1,....,hostN:port (missing port
     *     default to MariaDB default 3306 2 - host:port,...,host:port
     * @param haMode High availability mode
     * @return parsed endpoints
     */
    public static List<HostAddress> parse(String spec, HaMode haMode) {
    if (spec == null) {
      throw new IllegalArgumentException("Invalid connection URL, host address must not be empty ");
    }
    if ("".equals(spec)) {
      return new ArrayList<>(0);
    }
    String[] tokens = spec.trim().split(",");
    int size = tokens.length;
    List<HostAddress> arr = new ArrayList<>(size);

    // Aurora using cluster end point mustn't have any other host
    if (haMode == HaMode.AURORA) {
      Pattern clusterPattern =
          Pattern.compile(
              "(.+)\\.(?:cluster-|cluster-ro-)([a-z0-9]+\\.[a-z0-9\\-]+\\.rds\\.amazonaws\\.com)",
              Pattern.CASE_INSENSITIVE);
      Matcher matcher = clusterPattern.matcher(spec);

      if (!matcher.find()) {
        logger.warn(
            "Aurora recommended connection URL must only use cluster end-point like "
                + "\"jdbc:oceanbase:aurora://xx.cluster-yy.zz.rds.amazonaws.com\". "
                + "Using end-point permit auto-discovery of new replicas");
      }
    }

    for (String token : tokens) {
      if (token.startsWith("address=")) {
        arr.add(parseParameterHostAddress(token));
      } else {
        arr.add(parseSimpleHostAddress(token));
      }
    }

    if (haMode == HaMode.REPLICATION) {
      for (int i = 0; i < size; i++) {
        if (i == 0 && arr.get(i).type == null) {
          arr.get(i).type = ParameterConstant.TYPE_MASTER;
        } else if (i != 0 && arr.get(i).type == null) {
          arr.get(i).type = ParameterConstant.TYPE_SLAVE;
        }
      }
    }
    return arr;
  }

    private static HostAddress parseSimpleHostAddress(String str) {
        HostAddress result = new HostAddress();
        if (str.charAt(0) == '[') {
            /* IPv6 addresses in URLs are enclosed in square brackets */
            int ind = str.indexOf(']');
            result.host = str.substring(1, ind);
            if (ind != (str.length() - 1) && str.charAt(ind + 1) == ':') {
                result.port = getPort(str.substring(ind + 2));
            }
        } else if (str.contains(":")) {
            /* Parse host:port */
            String[] hostPort = str.split(":");
            result.host = hostPort[0];
            result.port = getPort(hostPort[1]);
        } else {
            /* Just host name is given */
            result.host = str;
            result.port = 3306;
        }
        return result;
    }

    private static int getPort(String portString) {
        try {
            return Integer.parseInt(portString);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Incorrect port value : " + portString);
        }
    }

    private static HostAddress parseParameterHostAddress(String str) {
        HostAddress result = new HostAddress();
        String[] array = str.split("(?=\\()|(?<=\\))");
        for (int i = 1; i < array.length; i++) {
            String[] token = array[i].replace("(", "").replace(")", "").trim().split("=");
            if (token.length != 2) {
                throw new IllegalArgumentException(
                    "Invalid connection URL, expected key=value pairs, found " + array[i]);
            }
            String key = token[0].toLowerCase();
            String value = token[1].toLowerCase();
            if ("host".equals(key)) {
                result.host = value.replace("[", "").replace("]", "");
            } else if ("port".equals(key)) {
                result.port = getPort(value);
            } else if ("type".equals(key)
                       && (value.equals(ParameterConstant.TYPE_MASTER) || value
                           .equals(ParameterConstant.TYPE_SLAVE))) {
                result.type = value;
            }
        }
        return result;
    }

    /**
     * ToString implementation of addresses.
     *
     * @param addrs address list
     * @return String value
     */
    public static String toString(List<HostAddress> addrs) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < addrs.size(); i++) {
            if (addrs.get(i).type != null) {
                str.append("address=(host=").append(addrs.get(i).host).append(")(port=")
                    .append(addrs.get(i).port).append(")(type=").append(addrs.get(i).type)
                    .append(")");
            } else {
                boolean isIPv6 = addrs.get(i).host != null && addrs.get(i).host.contains(":");
                String host = (isIPv6) ? ("[" + addrs.get(i).host + "]") : addrs.get(i).host;
                str.append(host).append(":").append(addrs.get(i).port);
            }
            if (i < addrs.size() - 1) {
                str.append(",");
            }
        }
        return str.toString();
    }

    /**
     * ToString implementation of addresses.
     *
     * @param addrs address array
     * @return String value
     */
    @SuppressWarnings("unused")
    public static String toString(HostAddress[] addrs) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < addrs.length; i++) {
            if (addrs[i].type != null) {
                str.append("address=(host=").append(addrs[i].host).append(")(port=")
                    .append(addrs[i].port).append(")(type=").append(addrs[i].type).append(")");
            } else {
                boolean isIPv6 = addrs[i].host != null && addrs[i].host.contains(":");
                String host = (isIPv6) ? ("[" + addrs[i].host + "]") : addrs[i].host;
                str.append(host).append(":").append(addrs[i].port);
            }
            if (i < addrs.length - 1) {
                str.append(",");
            }
        }
        return str.toString();
    }

    @Override
    public String toString() {
        return "HostAddress{" + "host='" + host + '\'' + ", port=" + port
               + ((type != null) ? (", type='" + type + "'") : "") + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        HostAddress that = (HostAddress) obj;

        return port == that.port
               && (host != null ? host.equals(that.host) : that.host == null
                                                           && !(type != null ? !type
                                                               .equals(that.type)
                                                               : that.type != null));
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }
}
