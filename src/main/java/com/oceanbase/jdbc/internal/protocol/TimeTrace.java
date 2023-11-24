/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2023 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc.internal.protocol;

import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;

public class TimeTrace {

    private static final Logger logger = LoggerFactory.getLogger("JDBC-COST-LOGGER");

    private long                serverThreadId;
    private int                 logTraceTimeMs;

    private long                startCallInterface;
    private long                endCallInterface;

    private long                startSendRequest;
    private long                endSendRequest;

    private long                startReceiveResponse;
    private long                endReceiveResponse;

    public TimeTrace(long serverThreadId, int logTraceTime) {
        this.serverThreadId = serverThreadId;
        this.logTraceTimeMs = logTraceTime;
    }

    public void startCallInterface() {
        try {
            startCallInterface = System.nanoTime();
        } catch (Exception e) {
            try {
                logger.warn("startCallInterface failed.", e);
            } catch (Exception ignore) {
            }
        }
    }

    public void endCallInterface(String interfaceName) {
        try {
            endCallInterface = System.nanoTime();
            long timeUs = getCallInterfaceElapsedTimeUs();
            if (logTraceTimeMs > 0 && logTraceTimeMs > (timeUs / 1000)) {
                return;
            }

            logger.info("[connectionId={}] {}: CallInterface costs {}us.", serverThreadId, interfaceName, timeUs);
        } catch (Exception e) {
            try {
                logger.warn("endCallInterface failed.", e);
            } catch (Exception ignore) {
            }
        }
    }

    private long getCallInterfaceElapsedTimeUs() {
        return (endCallInterface - startCallInterface) / 1000;
    }

    public void startSendRequest() {
        try {
            startSendRequest = System.nanoTime();
        } catch (Exception e) {
            try {
                logger.warn("startSendRequest failed.", e);
            } catch (Exception ignore) {
            }
        }
    }

    public void endSendRequest() {
        try {
            endSendRequest = System.nanoTime();
        } catch (Exception e) {
            try {
                logger.warn("endSendRequest failed.", e);
            } catch (Exception ignore) {
            }
        }
    }

    private long getSendRequestElapsedTimeUs() {
        return (endSendRequest - startSendRequest) / 1000;
    }

    public void startReceiveResponse() {
        try {
            startReceiveResponse = System.nanoTime();
        } catch (Exception e) {
            try {
                logger.warn("startReceiveResponse failed.", e);
            } catch (Exception ignore) {
            }
        }
    }

    public void endReceiveResponse(String protocol, String sql) {
        try {
            endReceiveResponse = System.nanoTime();
            long timeUs1 = getSendRequestElapsedTimeUs();
            long timeUs2 = getReceiveResponseElapsedTimeUs();
            if (logTraceTimeMs > 0 && logTraceTimeMs > (timeUs1 / 1000) && logTraceTimeMs > (timeUs2 / 1000)) {
                return;
            }

            if (sql == null) {
                sql = "";
            } else if (sql.length() > 100) {
                sql = sql.substring(0, 100) + "...";
            }
            logger.info("[connectionId={}] {}: SendRequest costs {}us, ReceiveResponse costs {}us. {}", serverThreadId, protocol,
                    getSendRequestElapsedTimeUs(), getReceiveResponseElapsedTimeUs(), sql);
        } catch (Exception e) {
            try {
                logger.warn("endReceiveResponse failed.", e);
            } catch (Exception ignore) {
            }
        }
    }

    private long getReceiveResponseElapsedTimeUs() {
        return (endReceiveResponse - startReceiveResponse) / 1000;
    }

}
