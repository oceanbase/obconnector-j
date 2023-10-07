package com.oceanbase.jdbc.internal.protocol;

import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;

public class TimeTrace {

    private static final Logger logger = LoggerFactory.getLogger("JDBC-COST-LOGGER");

    private long                startCallInterface;
    private long                endCallInterface;

    private long                startSendRequest;
    private long                endSendRequest;

    private long                startReceiveResponse;
    private long                endReceiveResponse;

    public void startCallInterface() {
        startCallInterface = System.nanoTime();
    }

    public void endCallInterface(String interfaceName) {
        endCallInterface = System.nanoTime();
        logger
            .info("{}: CallInterface costs {}us.", interfaceName, getCallInterfaceElapsedTimeUs());
    }

    public long getCallInterfaceElapsedTimeUs() {
        return (endCallInterface - startCallInterface) / 1000;
    }

    public void startSendRequest() {
        startSendRequest = System.nanoTime();
    }

    public void endSendRequest() {
        endSendRequest = System.nanoTime();
    }

    public long getSendRequestElapsedTimeUs() {
        return (endSendRequest - startSendRequest) / 1000;
    }

    public void startReceiveResponse() {
        startReceiveResponse = System.nanoTime();
    }

    public void endReceiveResponse(String protocol, String sql) {
        endReceiveResponse = System.nanoTime();
        if (sql == null) {
            sql = "";
        } else if (sql.length() > 100) {
            sql = sql.substring(0, 100) + "...";
        }
        logger.info("{}: SendRequest costs {}us, ReceiveResponse costs {}us. {}", protocol,
            getSendRequestElapsedTimeUs(), getReceiveResponseElapsedTimeUs(), sql);
    }

    public long getReceiveResponseElapsedTimeUs() {
        return (endReceiveResponse - startReceiveResponse) / 1000;
    }

}
