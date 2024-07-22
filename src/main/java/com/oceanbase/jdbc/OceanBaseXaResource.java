/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
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
package com.oceanbase.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.oceanbase.jdbc.extend.datatype.ComplexDataType;
import com.oceanbase.jdbc.extend.datatype.StructImpl;
import com.oceanbase.jdbc.internal.util.Utils;

public class OceanBaseXaResource implements XAResource {

    private static final int          MAX_COMMAND_LENGTH = 300;
    public static final int           TMMIGRATE          = 1048576;

    private final OceanBaseConnection connection;
    private boolean                   isChangedCommit;
    public static final int           ORATMSERIALIZABLE  = 1024;
    public static final int           ORATRANSLOOSE      = 65536;
    public static final int           ORATMREADONLY      = 256;

    public OceanBaseXaResource(OceanBaseConnection connection) {
        this.connection = connection;
    }

    protected static String xidToString(Xid xid) {
        return "0x" + Utils.byteArrayToHexString(xid.getGlobalTransactionId()) + ",0x"
               + Utils.byteArrayToHexString(xid.getBranchQualifier()) + ",0x"
               + Utils.intToHexString(xid.getFormatId());
    }

    private static String flagsToString(int flags) {
        switch (flags) {
            case TMJOIN:
                return "JOIN";
            case TMONEPHASE:
                return "ONE PHASE";
            case TMRESUME:
                return "RESUME";
            case TMSUSPEND:
                return "SUSPEND";
            default:
                return "";
        }
    }

    private XAException mapXaException(SQLException sqle) {
        int xaErrorCode;

        switch (sqle.getErrorCode()) {
            case 1397:
                xaErrorCode = XAException.XAER_NOTA;
                break;
            case 1398:
                xaErrorCode = XAException.XAER_INVAL;
                break;
            case 1399:
                xaErrorCode = XAException.XAER_RMFAIL;
                break;
            case 1400:
                xaErrorCode = XAException.XAER_OUTSIDE;
                break;
            case 1401:
                xaErrorCode = XAException.XAER_RMERR;
                break;
            case 1402:
                xaErrorCode = XAException.XA_RBROLLBACK;
                break;
            default:
                xaErrorCode = 0;
                break;
        }
        XAException xaException;
        if (xaErrorCode != 0) {
            xaException = new XAException(xaErrorCode);

        } else {
            xaException = new XAException(sqle.getMessage());
        }
        xaException.initCause(sqle);
        return xaException;
    }

    private XAException mapXaException2(SQLException sqle) {
        XAException xaException;
        xaException = new XAException(XAException.XAER_RMFAIL);
        xaException.initCause(sqle);
        return xaException;
    }

    /**
     * Execute a query.
     *
     * @param command query to run.
     * @throws XAException exception
     */
    private void execute(String command) throws XAException {
        try {
            connection.createStatement().execute(command);
        } catch (SQLException sqle) {
            throw mapXaException(sqle);
        }
    }

    /**
     * Commits the global transaction specified by xid.
     *
     * @param xid A global transaction identifier
     * @param onePhase If true, the resource manager should use a one-phase commit protocol to commit
     *     the work done on behalf of xid.
     * @throws XAException exception
     */
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().startCallInterface();
        }

        if (this.connection.getProtocol().isOracleMode()) {
            StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
            commandBuf.append("select DBMS_XA.XA_COMMIT(?, ?) from dual");
            ObStruct xidObj = genOracleXid(xid);
            try {
                dispatchOracleCommand(commandBuf.toString(), xidObj, Boolean.valueOf(onePhase));
            } finally {
                this.connection.setInGlobalTx(false);
            }
        } else {
            String command = "XA COMMIT " + xidToString(xid);
            if (onePhase) {
                command += " ONE PHASE";
            }
            execute(command);
        }

        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().endCallInterface("OceanBaseXaResource.commit");
        }
    }

    /**
     * Ends the work performed on behalf of a transaction branch. The resource manager disassociates
     * the XA resource from the transaction branch specified and lets the transaction complete.
     *
     * <p>If TMSUSPEND is specified in the flags, the transaction branch is temporarily suspended in
     * an incomplete state. The transaction context is in a suspended state and must be resumed via
     * the start method with TMRESUME specified.
     *
     * <p>If TMFAIL is specified, the portion of work has failed. The resource manager may mark the
     * transaction as rollback-only
     *
     * <p>If TMSUCCESS is specified, the portion of work has completed successfully.
     *
     * @param xid A global transaction identifier that is the same as the identifier used previously
     *     in the start method.
     * @param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND.
     * @throws XAException An error has occurred. (XAException values are XAER_RMERR, XAER_RMFAILED,
     *     XAER_NOTA, XAER_INVAL, XAER_PROTO, or XA_RB*)
     */
    public void end(Xid xid, int flags) throws XAException {
        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().startCallInterface();
        }

        if (this.connection.getProtocol().isOracleMode()) {
            if ((flags & TMSUCCESS) == 0 && (flags & TMSUSPEND) == 0 && (flags & TMFAIL) == 0
                && (flags & TMMIGRATE) == 0) {
                throw new XAException(XAException.XAER_INVAL);
            }
            if (flags == TMFAIL) {
                flags = TMSUCCESS;
            }

            StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
            commandBuf.append("select DBMS_XA.XA_END(?, ?) from dual");
            ObStruct xidObj = genOracleXid(xid);

            dispatchOracleCommand(commandBuf.toString(), xidObj, Integer.valueOf(flags));

            /**
             *   Only when XA END is executed correctly does it need to be reset,
             *   otherwise the XA transaction is still in the transaction
             */
            try {
                OceanBaseConnection mySQLConnection = this.connection;
                if (this.isChangedCommit) {
                    mySQLConnection.setAutoCommit(true);
                    this.isChangedCommit = false;
                }
            } catch (SQLException e) {
                throw mapXaException2(e);
            }
        } else {
            if (flags != TMSUCCESS && flags != TMSUSPEND && flags != TMFAIL) {
                throw new XAException(XAException.XAER_INVAL);
            }
            execute("XA END " + xidToString(xid) + " " + flagsToString(flags));
        }

        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().endCallInterface("OceanBaseXaResource.end");
        }
    }

    /**
     * Tells the resource manager to forget about a heuristically completed transaction branch.
     *
     * @param xid A global transaction identifier.
     */
    public void forget(Xid xid) {
        // Not implemented by the server
    }

    /**
     * Obtains the current transaction timeout value set for this XAResource instance. If
     * XAResource.setTransactionTimeout was not used prior to invoking this method, the return value
     * is the default timeout set for the resource manager; otherwise, the value used in the previous
     * setTransactionTimeout call is returned.
     *
     * @return the transaction timeout value in seconds.
     */
    public int getTransactionTimeout() {
        // not implemented
        return 0;
    }

    /**
     * This method is called to determine if the resource manager instance represented by the target
     * object is the same as the resource manager instance represented by the parameter xares.
     *
     * @param xaResource An XAResource object whose resource manager instance is to be compared with
     *     the target object.
     * @return true if it's the same RM instance; otherwise false.
     */
    @Override
    public boolean isSameRM(XAResource xaResource) {
        // Typically used by transaction manager to "join" transactions. We do not support joins,
        // so always return false;
        return false;
    }

    /**
     * Ask the resource manager to prepare for a transaction commit of the transaction specified in
     * xid.
     *
     * @param xid A global transaction identifier.
     * @return A value indicating the resource manager's vote on the outcome of the transaction.
     * @throws XAException An error has occurred. Possible exception values are: XA_RB*, XAER_RMERR,
     *     XAER_RMFAIL, XAER_NOTA, XAER_INVAL, XAER_PROTO.
     */
    public int prepare(Xid xid) throws XAException {
        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().startCallInterface();
        }

        int xaRet;
        if (this.connection.getProtocol().isOracleMode()) {
            StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
            commandBuf.append("select DBMS_XA.XA_PREPARE(?) from dual");
            ObStruct xidObj = genOracleXid(xid);
            xaRet = dispatchOracleCommand(commandBuf.toString(), xidObj);
        } else {
            execute("XA PREPARE " + xidToString(xid));
            xaRet = XA_OK;
        }

        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().endCallInterface("OceanBaseXaResource.prepare");
        }
        return xaRet;
    }

    /**
     * Obtains a list of prepared transaction branches from a resource manager. The transaction
     * manager calls this method during recovery to obtain the list of transaction branches that are
     * currently in prepared or heuristically completed states.
     *
     * @param flags One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS must be used when no other
     *     flags are set in the parameter.
     * @return The resource manager returns zero or more XIDs of the transaction branches.
     * @throws XAException An error has occurred. Possible values are XAER_RMERR, XAER_RMFAIL,
     *     XAER_INVAL, and XAER_PROTO.
     */
    public Xid[] recover(int flags) throws XAException {
    // Return all Xid  at once, when STARTRSCAN is specified
    // Return zero-length array otherwise.
    if (((flags & TMSTARTRSCAN) == 0) && ((flags & TMENDRSCAN) == 0) && (flags != TMNOFLAGS)) {
      throw new XAException(XAException.XAER_INVAL);
    }
    if ((flags & TMSTARTRSCAN) == 0) {
      return new OceanBaseXid[0];
    }
    if (connection != null && connection.getProtocol() != null) {
        connection.getProtocol().startCallInterface();
    }

    try {
        if (this.connection.getProtocol().isOracleMode()) {
            List<OceanBaseXid> recoveredXidList = new ArrayList<OceanBaseXid>();
            java.sql.PreparedStatement psStmt = null;
            ResultSet rs = null;
            ResultSet arrayRes = null;
            String command = "declare " + "  x DBMS_XA_XID_ARRAY := DBMS_XA_XID_ARRAY(); "
                    + "BEGIN " + "  x := DBMS_XA.XA_RECOVER(); " + "  ? := x;" + "END;";
            try {
                OceanBaseConnection mySQLConnection = this.connection;
                // TODO: Cache this for lifetime of XAConnection
                psStmt = mySQLConnection.prepareStatement(command);
                psStmt.setNull(1, java.sql.Types.NULL);
                psStmt.execute();
                rs = ((OceanBaseStatement)psStmt).getResults().getCallableResultSet();
                while (rs.next()) {
                    java.sql.Array array = rs.getArray(1);
                    arrayRes = array.getResultSet();
                    while (arrayRes.next()) {
                        java.sql.Struct struct = (java.sql.Struct) arrayRes.getObject(2);
                        Object[] objArr = struct.getAttributes();
                        recoveredXidList.add(new OceanBaseXid((byte[]) objArr[1],
                                (byte[]) objArr[2], ((BigDecimal) objArr[0]).intValue()));
                    }
                    array.free();
                }
            } catch (SQLException sqlEx) {
                throw new XAException(XAException.XAER_RMFAIL);
            } finally {
                try {
                    if (arrayRes != null) {
                        arrayRes.close();
                    }
                    if (rs != null) {
                        rs.close();
                    }
                    if (psStmt != null) {
                        psStmt.close();
                    }
                } catch (SQLException sqlEx) { // NOPMD
                    throw mapXaException2(sqlEx);
                }
            }
            int numXids = recoveredXidList.size();
            Xid[] asXids = new Xid[numXids];
            Object[] asObjects = recoveredXidList.toArray();
            for (int i = 0; i < numXids; i++) {
                asXids[i] = (Xid) asObjects[i];
            }
            return asXids;
        } else {
            try (ResultSet rs = connection.createStatement().executeQuery("XA RECOVER")) {
                ArrayList<OceanBaseXid> xidList = new ArrayList<>();
                while (rs.next()) {
                    int formatId = rs.getInt(1);
                    int len1 = rs.getInt(2);
                    int len2 = rs.getInt(3);
                    byte[] arr = rs.getBytes(4);
                    byte[] globalTransactionId = new byte[len1];
                    byte[] branchQualifier = new byte[len2];
                    System.arraycopy(arr, 0, globalTransactionId, 0, len1);
                    System.arraycopy(arr, len1, branchQualifier, 0, len2);
                    xidList.add(new OceanBaseXid(formatId, globalTransactionId, branchQualifier));
                }
                Xid[] xids = new Xid[xidList.size()];
                xidList.toArray(xids);
                return xids;
            } catch (SQLException sqle) {
                throw mapXaException(sqle);
            }
        }
    } finally {
        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().endCallInterface("OceanBaseConnection.recover");
        }
    }
  }

    /**
     * Informs the resource manager to roll back work done on behalf of a transaction branch.
     *
     * @param xid A global transaction identifier.
     * @throws XAException An error has occurred.
     */
    public void rollback(Xid xid) throws XAException {
        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().startCallInterface();
        }

        if (this.connection.getProtocol().isOracleMode()) {
            StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
            commandBuf.append("select DBMS_XA.XA_ROLLBACK(?) from dual");
            ObStruct xidObj = genOracleXid(xid);
            try {
                dispatchOracleCommand(commandBuf.toString(), xidObj);
            } finally {
                this.connection.setInGlobalTx(false);
            }
        } else {
            execute("XA ROLLBACK " + xidToString(xid));
        }

        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().endCallInterface("OceanBaseXaResource.rollback");
        }
    }

    /**
     * Sets the current transaction timeout value for this XAResource instance. Once set, this timeout
     * value is effective until setTransactionTimeout is invoked again with a different value. To
     * reset the timeout value to the default value used by the resource manager, set the value to
     * zero. If the timeout operation is performed successfully, the method returns true; otherwise
     * false. If a resource manager does not support explicitly setting the transaction timeout value,
     * this method returns false.
     *
     * @param timeout The transaction timeout value in seconds.
     * @return true if the transaction timeout value is set successfully; otherwise false.
     */
    public boolean setTransactionTimeout(int timeout) {
        return false; // not implemented
    }

    /**
     * Starts work on behalf of a transaction branch specified in xid. If TMJOIN is specified, the
     * start applies to joining a transaction previously seen by the resource manager. If TMRESUME is
     * specified, the start applies to resuming a suspended transaction specified in the parameter
     * xid. If neither TMJOIN nor TMRESUME is specified and the transaction specified by xid has
     * previously been seen by the resource manager, the resource manager throws the XAException
     * exception with XAER_DUPID error code.
     *
     * @param xid A global transaction identifier to be associated with the resource.
     * @param flags One of TMNOFLAGS, TMJOIN, or TMRESUME.
     * @throws XAException An error has occurred.
     */
    public void start(Xid xid, int flags) throws XAException {
        if (flags != TMJOIN && flags != TMRESUME && flags != TMNOFLAGS
            && flags != ORATMSERIALIZABLE && flags != ORATRANSLOOSE && flags != ORATMREADONLY) {
            throw new XAException(XAException.XAER_INVAL);
        }
        if (connection != null && connection.getProtocol() != null) {
            connection.getProtocol().startCallInterface();
        }

        try {
            if (this.connection.getProtocol().isOracleMode()) {
                StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
                commandBuf.append("select DBMS_XA.XA_START(?, ?) from dual");
                ObStruct xidObj = genOracleXid(xid);

                try {
                    OceanBaseConnection mySQLConnection = this.connection;
                    boolean isAutoCommit = mySQLConnection.getAutoCommit();
                    if (isAutoCommit) {
                        mySQLConnection.setAutoCommit(false);
                        this.isChangedCommit = true;
                    } else {
                        this.isChangedCommit = false;
                    }
                } catch (SQLException e) {
                    throw mapXaException2(e);
                }
                try {
                    dispatchOracleCommand(commandBuf.toString(), xidObj, Integer.valueOf(flags));
                } catch (XAException e) {
                    try {
                        OceanBaseConnection mySQLConnection = this.connection;
                        if (this.isChangedCommit) {
                            mySQLConnection.setAutoCommit(true);
                            this.isChangedCommit = false;
                        }
                    } catch (SQLException se) { // NOPMD
                        throw mapXaException2(se);
                    }
                    throw e;
                }
            } else {
                execute("XA START "
                        + xidToString(xid)
                        + " "
                        + flagsToString(flags == TMJOIN
                                        && connection.getPinGlobalTxToPhysicalConnection() ? TMRESUME
                            : flags));
            }
        } finally {
            if (connection != null && connection.getProtocol() != null) {
                connection.getProtocol().endCallInterface("OceanBaseXaResource.start");
            }
        }
    }

    private ObStruct genOracleXid(Xid xid) throws XAException {
        try {
            OceanBaseConnection mySQLConnection = this.connection;
            Object[] xidObj = new Object[3];
            int i = 0;
            xidObj[i++] = xid.getFormatId();
            xidObj[i++] = xid.getGlobalTransactionId();
            xidObj[i++] = xid.getBranchQualifier();
            ComplexDataType complexType = null;

            if (this.connection.getProtocol().getOptions().useLocalXID) {
                String childTypeName = "DBMS_XA_XID";
                String ownerName = "SYS";
                int type = 3;
                complexType = new ComplexDataType(childTypeName, ownerName, type);
                complexType.setValid(true);
                complexType.setAttrCount(3);
                complexType.setAttrType(0, new ComplexDataType("NUMBER", "", 0));
                complexType.incInitAttrCount();
                complexType.setAttrType(1, new ComplexDataType("RAW", "", 6));
                complexType.incInitAttrCount();
                complexType.setAttrType(2, new ComplexDataType("RAW", "", 6));
                complexType.incInitAttrCount();
                ObStruct struct = new StructImpl(complexType);
                struct.setAttrData(xidObj);
                return struct;
            } else {
                return (ObStruct) mySQLConnection.createStruct("DBMS_XA_XID", xidObj);
            }
        } catch (SQLException sqlEx) {
            throw mapXaException2(sqlEx);
        }
    }

    private int dispatchOracleCommand(String command, ObStruct xid) throws XAException {
        return dispatchOracleCommand(command, xid, null);
    }

    private int dispatchOracleCommand(String command, ObStruct xid, Object param)
                                                                                 throws XAException {
        java.sql.PreparedStatement psStmt = null;
        try {

            OceanBaseConnection mySQLConnection = this.connection;

            // TODO: Cache this for lifetime of XAConnection
            psStmt = mySQLConnection.prepareStatement(command);

            psStmt.setObject(1, xid);
            if (param != null) {
                psStmt.setObject(2, param);
            }
            ResultSet rs = null;
            try {
                rs = psStmt.executeQuery();
                // Whether the execution succeeds or fails, a result set will be returned
                if (rs.next()) {
                    int xaRet = rs.getInt(1);

                    if (xaRet < 0) {
                        XAException xaException = new XAException(xaRet);
                        throw xaException;
                    }
                    return xaRet;
                } else {
                    throw new XAException(XAException.XAER_RMFAIL);
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }

        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw mapXaException2(sqlEx);
        } finally {
            if (psStmt != null) {
                try {
                    psStmt.close();
                } catch (SQLException sqlEx) { // NOPMD
                    throw mapXaException2(sqlEx);
                }
            }
        }
    }
}
