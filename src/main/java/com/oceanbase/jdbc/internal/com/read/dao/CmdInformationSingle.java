/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc.internal.com.read.dao;

import java.sql.ResultSet;

import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.Utils;

public class CmdInformationSingle implements CmdInformation {

    private final long insertId;
    private final int  autoIncrement;
    private long       updateCount;
    private String     serverInfo;

    /**
     * Object containing update / insert ids, optimized for only one result.
     *
     * @param insertId auto generated id.
     * @param updateCount update count
     * @param autoIncrement connection auto increment value.
     */
    public CmdInformationSingle(long insertId, long updateCount, int autoIncrement) {
        this.insertId = insertId;
        this.updateCount = updateCount;
        this.autoIncrement = autoIncrement;
    }

    public CmdInformationSingle(long insertId, long updateCount, int autoIncrement,
                                String serverInfo) {
        this.insertId = insertId;
        this.updateCount = updateCount;
        this.autoIncrement = autoIncrement;
        this.serverInfo = serverInfo;
    }

    public long getInsertId() {
        return insertId;
    }

    @Override
    public int[] getUpdateCounts() {
        return new int[] { (int) updateCount };
    }

    @Override
    public int[] getServerUpdateCounts() {
        return new int[] { (int) updateCount };
    }

    @Override
    public long[] getLargeUpdateCounts() {
        return new long[] { updateCount };
    }

    @Override
    public int getUpdateCount() {
        return (int) updateCount;
    }

    @Override
    public long getLargeUpdateCount() {
        return updateCount;
    }

    @Override
    public String getServerInfo() {
        return serverInfo;
    }

    @Override
    public void addErrorStat() {
        // not expected
    }

    @Override
    public void reset() {
        // not expected
    }

    @Override
    public void addResultSetStat() {
        // not expected
    }

    /**
     * Get generated Keys.
     *
     * @param protocol current protocol
     * @param sql SQL command
     * @return a resultSet containing the single insert ids.
     */
    public ResultSet getGeneratedKeys(Protocol protocol, String sql) {
        if (insertId == 0) {
            // for update sql
            long[] insertIds = new long[0];
            return SelectResultSet.createGeneratedData(insertIds, protocol, true);
        }
        if (updateCount > 1 && sql != null && !isDuplicateKeyUpdate(sql)) {
            long[] insertIds = new long[(int) updateCount];
            for (int i = 0; i < updateCount; i++) {
                insertIds[i] = insertId + i * autoIncrement;
            }
            return SelectResultSet.createGeneratedData(insertIds, protocol, true);
        }
        return SelectResultSet.createGeneratedData(new long[] { insertId }, protocol, true);
    }

    private boolean isDuplicateKeyUpdate(String sql) {
        Utils.TrimSQLInfo trimSQLStringInternal = Utils.trimSQLStringInternal(sql, false, false,
            false);
        return trimSQLStringInternal.getTrimedString().matches(
            "(?i).*ON\\s+DUPLICATE\\s+KEY\\s+UPDATE.*");
    }

    @Override
    public ResultSet getBatchGeneratedKeys(Protocol protocol) {
        return getGeneratedKeys(protocol, null);
    }

    public int getCurrentStatNumber() {
        return 1;
    }

    @Override
    public boolean moreResults() {
        updateCount = RESULT_SET_VALUE;
        return false;
    }

    public boolean isCurrentUpdateCount() {
        return updateCount != RESULT_SET_VALUE;
    }

    @Override
    public void addSuccessStat(long updateCount, long insertId) {
        // cannot occur
    }

    @Override
    public void addSuccessStat(long updateCount, long insertId, boolean containOnDuplicateKey,
                               String serverInfo) {
        // cannot occur
    }

    public void setRewrite(boolean rewritten) {
        // no need
    }

    public boolean getRewrite() {
        return false;
    }
}
