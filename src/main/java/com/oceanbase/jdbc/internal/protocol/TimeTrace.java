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
