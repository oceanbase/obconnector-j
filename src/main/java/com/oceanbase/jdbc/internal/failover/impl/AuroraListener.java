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
package com.oceanbase.jdbc.internal.failover.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.oceanbase.jdbc.internal.protocol.AuroraProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.ReconnectDuringTransactionException;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;

public class AuroraListener extends MastersSlavesListener {

    private static final Logger logger           = Logger.getLogger(AuroraListener.class.getName());
    private final Pattern       auroraDnsPattern = Pattern
                                                     .compile(
                                                         "(.+)\\.(cluster-|cluster-ro-)?([a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
                                                         Pattern.CASE_INSENSITIVE);
    private final HostAddress   clusterHostAddress;
    private String              clusterDnsSuffix = null;

    /**
     * Constructor for Aurora. This differ from standard failover because : - we don't know current
     * master, we must check that after initial connection - master can change after he has a failover
     *
     * @param urlParser connection information
     * @param globalInfo server global variables information
     * @throws SQLException when connection string contain host with different cluster
     */
    public AuroraListener(UrlParser urlParser, final GlobalStateInfo globalInfo)
                                                                                throws SQLException {
        super(urlParser, globalInfo);
        clusterHostAddress = findClusterHostAddress();
    }

    /**
     * Retrieves the cluster host address from the UrlParser instance.
     *
     * @return cluster host address
     */
    private HostAddress findClusterHostAddress() throws SQLException {
        Matcher matcher;
        for (HostAddress hostAddress : hostAddresses) {
            matcher = auroraDnsPattern.matcher(hostAddress.host);
            if (matcher.find()) {

                if (clusterDnsSuffix != null) {
                    // ensure there is only one cluster
                    if (!clusterDnsSuffix.equalsIgnoreCase(matcher.group(3))) {
                        throw new SQLException(
                            "Connection string must contain only one aurora cluster. " + "'"
                                    + hostAddress.host + "' doesn't correspond to DNS prefix '"
                                    + clusterDnsSuffix + "'");
                    }
                } else {
                    clusterDnsSuffix = matcher.group(3);
                }

                if (matcher.group(2) != null && !matcher.group(2).isEmpty()) {
                    // not just an instance entry-point, but cluster entrypoint.
                    return hostAddress;
                }
            } else {
                if (clusterDnsSuffix == null && hostAddress.host.contains(".")
                    && !Utils.isIPv4(hostAddress.host) && !Utils.isIPv6(hostAddress.host)) {
                    clusterDnsSuffix = hostAddress.host
                        .substring(hostAddress.host.indexOf(".") + 1);
                }
            }
        }
        return null;
    }

    public String getClusterDnsSuffix() {
        return clusterDnsSuffix;
    }

    public HostAddress getClusterHostAddress() {
        return clusterHostAddress;
    }

    /**
     * Search a valid connection for failed one. A Node can be a master or a replica depending on the
     * cluster state. so search for each host until found all the failed connection. By default,
     * search for the host not down, and recheck the down one after if not found valid connections.
     *
     * @param initialSearchFilter initial search filter
     * @throws SQLException if a connection asked is not found
     */
    @Override
  public void reconnectFailedConnection(SearchFilter initialSearchFilter) throws SQLException {
    SearchFilter searchFilter = initialSearchFilter;
    if (!searchFilter.isInitialConnection()
        && (isExplicitClosed()
            || (searchFilter.isFineIfFoundOnlyMaster() && !isMasterHostFail())
            || searchFilter.isFineIfFoundOnlySlave() && !isSecondaryHostFail())) {
      return;
    }

    if (!searchFilter.isFailoverLoop()) {
      try {
        checkWaitingConnection();
        if ((searchFilter.isFineIfFoundOnlyMaster() && !isMasterHostFail())
            || searchFilter.isFineIfFoundOnlySlave() && !isSecondaryHostFail()) {
          return;
        }
      } catch (ReconnectDuringTransactionException e) {
        // don't throw an exception for this specific exception
        return;
      }
    }

    currentConnectionAttempts.incrementAndGet();

    resetOldsBlackListHosts();

    // put the list in the following order
    // - random order not connected host and not blacklisted
    // - random blacklisted host
    // - connected host at end.
    List<HostAddress> loopAddress = new LinkedList<>(hostAddresses);
    loopAddress.removeAll(getBlacklistKeys());
    Collections.shuffle(loopAddress);
    List<HostAddress> blacklistShuffle = new LinkedList<>(getBlacklistKeys());
    blacklistShuffle.retainAll(hostAddresses);
    Collections.shuffle(blacklistShuffle);
    loopAddress.addAll(blacklistShuffle);

    // put connected at end
    if (masterProtocol != null && !isMasterHostFail()) {
      loopAddress.remove(masterProtocol.getHostAddress());
      loopAddress.add(masterProtocol.getHostAddress());
    }

    if (!isSecondaryHostFail() && secondaryProtocol != null) {
      loopAddress.remove(secondaryProtocol.getHostAddress());
      loopAddress.add(secondaryProtocol.getHostAddress());
    }

    if (hostAddresses.size() <= 1) {
      searchFilter = new SearchFilter(true, false);
    }
    if ((isMasterHostFail() || isSecondaryHostFail()) || searchFilter.isInitialConnection()) {
      // while permit to avoid case when succeeded creating a new Master connection
      // and ping master connection fail a few milliseconds after,
      // resulting a masterConnection not initialized.
      do {
        AuroraProtocol.loop(this, globalInfo, loopAddress, searchFilter);
        if (!searchFilter.isFailoverLoop()) {
          try {
            checkWaitingConnection();
          } catch (ReconnectDuringTransactionException e) {
            // don't throw an exception for this specific exception
          }
        }
      } while (searchFilter.isInitialConnection()
          && !(masterProtocol != null
              || (urlParser.getOptions().allowMasterDownConnection && secondaryProtocol != null)));
    }

    // When reconnecting, search if replicas list has change since first initialisation
    if (getCurrentProtocol() != null && !getCurrentProtocol().isClosed()) {
      retrieveAllEndpointsAndSet(getCurrentProtocol());
    }

    if (searchFilter.isInitialConnection() && masterProtocol == null && !currentReadOnlyAsked) {
      currentProtocol = this.secondaryProtocol;
      currentReadOnlyAsked = true;
    }
  }

    /**
     * Retrieves the information necessary to add a new endpoint. Calls the methods that retrieves the
     * instance identifiers and sets urlParser accordingly.
     *
     * @param protocol current protocol connected to
     * @throws SQLException if connection error occur
     */
    public void retrieveAllEndpointsAndSet(Protocol protocol) throws SQLException {
        // For a given cluster, same port for all endpoints and same end host address
        if (clusterDnsSuffix != null) {
            List<String> endpoints = getCurrentEndpointIdentifiers(protocol);
            setUrlParserFromEndpoints(endpoints, protocol.getPort());
        }
    }

    /**
     * Retrieves all endpoints of a cluster from the appropriate database table.
     *
     * @param protocol current protocol connected to
     * @return instance endpoints of the cluster
     * @throws SQLException if connection error occur
     */
    private List<String> getCurrentEndpointIdentifiers(Protocol protocol) throws SQLException {
    List<String> endpoints = new ArrayList<>();
    try {
      proxy.lock.lock();
      lockLogger.debug("AuroraListener.getCurrentEndpointIdentifiers locked");
      try {
        // Deleted instance may remain in db for 24 hours so ignoring instances that have had no
        // change
        // for 3 minutes
        Results results = new Results();
        protocol.executeQuery(
            false,
            results,
            "select server_id, session_id from information_schema.replica_host_status "
                + "where last_update_timestamp > now() - INTERVAL 3 MINUTE");
        results.commandEnd();
        ResultSet resultSet = results.getResultSet();

        while (resultSet.next()) {
          endpoints.add(resultSet.getString(1) + "." + clusterDnsSuffix);
        }

        // randomize order for distributed load-balancing
        Collections.shuffle(endpoints);

      } finally {
        proxy.lock.unlock();
        lockLogger.debug("AuroraListener.getCurrentEndpointIdentifiers unlocked");
      }
    } catch (SQLException qe) {
      logger.warning("SQL exception occurred: " + qe.getMessage());
      if (protocol.getProxy().hasToHandleFailover(qe)) {
        if (masterProtocol == null || masterProtocol.equals(protocol)) {
          setMasterHostFail();
        } else if (secondaryProtocol.equals(protocol)) {
          setSecondaryHostFail();
        }
        addToBlacklist(protocol.getHostAddress());
        reconnectFailedConnection(new SearchFilter(isMasterHostFail(), isSecondaryHostFail()));
      }
    }

    return endpoints;
  }

    /**
     * Sets urlParser accordingly to discovered hosts.
     *
     * @param endpoints instance identifiers
     * @param port port that is common to all endpoints
     */
    private void setUrlParserFromEndpoints(List<String> endpoints, int port) {
    List<HostAddress> addresses = new ArrayList<>();
    for (String endpoint : endpoints) {
      if (endpoint != null) {
        addresses.add(new HostAddress(endpoint, port, null));
      }
    }
    if (addresses.isEmpty()) {
      addresses.addAll(urlParser.getHostAddresses());
    }
    hostAddresses = addresses;
  }

    /**
     * Looks for the current master/writer instance via the secondary protocol if it is found within 3
     * attempts. Should it not be able to connect, the host is blacklisted and null is returned.
     * Otherwise, it will open a new connection to the cluster endpoint and retrieve the data from
     * there.
     *
     * @param secondaryProtocol the current secondary protocol
     * @param loopAddress list of possible hosts
     * @return the probable master address or null if not found
     */
    public HostAddress searchByStartName(Protocol secondaryProtocol, List<HostAddress> loopAddress) {
        if (!isSecondaryHostFail()) {
            int checkWriterAttempts = 3;
            HostAddress currentWriter = null;

            do {
                try {
                    currentWriter = searchForMasterHostAddress(secondaryProtocol, loopAddress);
                } catch (SQLException qe) {
                    if (proxy.hasToHandleFailover(qe) && setSecondaryHostFail()) {
                        addToBlacklist(secondaryProtocol.getHostAddress());
                        return null;
                    }
                }
                checkWriterAttempts--;
            } while (currentWriter == null && checkWriterAttempts > 0);

            // Handling special case where no writer is found from secondaryProtocol
            if (currentWriter == null && getClusterHostAddress() != null) {
                AuroraProtocol possibleMasterProtocol = AuroraProtocol.getNewProtocol(getProxy(),
                    globalInfo, getUrlParser());
                possibleMasterProtocol.setHostAddress(getClusterHostAddress());
                try {
                    possibleMasterProtocol.connect();
                    if (possibleMasterProtocol.isMasterConnection()) {
                        possibleMasterProtocol.setMustBeMasterConnection(true);
                        foundActiveMaster(possibleMasterProtocol);
                    } else {
                        possibleMasterProtocol.setMustBeMasterConnection(false);
                    }
                } catch (IOException e) {
                    addToBlacklist(possibleMasterProtocol.getHostAddress());
                } catch (SQLException qe) {
                    if (proxy.hasToHandleFailover(qe)) {
                        addToBlacklist(possibleMasterProtocol.getHostAddress());
                    }
                }
            }

            return currentWriter;
        }
        return null;
    }

    /**
     * Aurora replica doesn't have the master endpoint but the master instance name. since the end
     * point normally use the instance name like
     * "instance-name.some_unique_string.region.rds.amazonaws.com", if an endpoint start with this
     * instance name, it will be checked first. Otherwise, the endpoint ending string is extracted and
     * used since the writer was newly created.
     *
     * @param protocol current protocol
     * @param loopAddress list of possible hosts
     * @return the probable host address or null if no valid endpoint found
     * @throws SQLException if any connection error occur
     */
    private HostAddress searchForMasterHostAddress(Protocol protocol, List<HostAddress> loopAddress)
                                                                                                    throws SQLException {
        String masterHostName;
        proxy.lock.lock();
        try {
            lockLogger.debug("AuroraListener.searchForMasterHostAddress locked");

            Results results = new Results();
            protocol.executeQuery(false, results,
                "select server_id from information_schema.replica_host_status "
                        + "where session_id = 'MASTER_SESSION_ID' "
                        + "and last_update_timestamp > now() - INTERVAL 3 MINUTE "
                        + "ORDER BY last_update_timestamp DESC LIMIT 1");
            results.commandEnd();
            ResultSet queryResult = results.getResultSet();

            if (!queryResult.isBeforeFirst()) {
                return null;
            } else {
                queryResult.next();
                masterHostName = queryResult.getString(1);
            }
        } finally {
            proxy.lock.unlock();
            lockLogger.debug("AuroraListener.searchForMasterHostAddress unlocked");
        }

        Matcher matcher;
        if (masterHostName != null) {
            for (HostAddress hostAddress : loopAddress) {
                matcher = auroraDnsPattern.matcher(hostAddress.host);
                if (hostAddress.host.startsWith(masterHostName) && !matcher.find()) {
                    return hostAddress;
                }
            }

            HostAddress masterHostAddress;
            if (clusterDnsSuffix == null && protocol.getHost().contains(".")) {
                clusterDnsSuffix = protocol.getHost()
                    .substring(protocol.getHost().indexOf(".") + 1);
            } else {
                return null;
            }

            masterHostAddress = new HostAddress(masterHostName + "." + clusterDnsSuffix,
                protocol.getPort(), null);
            loopAddress.add(masterHostAddress);
            if (!hostAddresses.contains(masterHostAddress)) {
                hostAddresses.add(masterHostAddress);
            }
            return masterHostAddress;
        }

        return null;
    }

    @Override
    public boolean checkMasterStatus(SearchFilter searchFilter) {
        if (!isMasterHostFail()) {
            try {
                if (masterProtocol != null && !masterProtocol.checkIfMaster()) {
                    // master has been demote, is now secondary
                    setMasterHostFail();
                    if (isSecondaryHostFail()) {
                        foundActiveSecondary(masterProtocol);
                    }
                    return true;
                }
            } catch (SQLException e) {
                try {
                    masterProtocol.ping();
                } catch (SQLException ee) {
                    proxy.lock.lock();
                    try {
                        lockLogger.debug("AuroraListener.checkMasterStatus !isMasterHostFail locked");
                        masterProtocol.close();
                    } finally {
                        proxy.lock.unlock();
                        lockLogger.debug("AuroraListener.checkMasterStatus !isMasterHostFail unlocked");
                    }
                    if (setMasterHostFail()) {
                        addToBlacklist(masterProtocol.getHostAddress());
                    }
                }
                return true;
            }
        }

        if (!isSecondaryHostFail()) {
            try {
                if (secondaryProtocol != null && secondaryProtocol.checkIfMaster()) {
                    // secondary has been promoted to master
                    setSecondaryHostFail();
                    if (isMasterHostFail()) {
                        foundActiveMaster(secondaryProtocol);
                    }
                    return true;
                }
            } catch (SQLException e) {
                try {
                    this.secondaryProtocol.ping();
                } catch (Exception ee) {
                    proxy.lock.lock();
                    try {
                        lockLogger.debug("AuroraListener.checkMasterStatus isMasterHostFail locked");
                        secondaryProtocol.close();
                    } finally {
                        proxy.lock.unlock();
                        lockLogger.debug("AuroraListener.checkMasterStatus isMasterHostFail unlocked");
                    }
                    if (setSecondaryHostFail()) {
                        addToBlacklist(this.secondaryProtocol.getHostAddress());
                    }
                    return true;
                }
            }
        }

        return false;
    }
}
