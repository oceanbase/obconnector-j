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
package com.oceanbase.jdbc.internal.failover.tools;

public class SearchFilter {

    private boolean fineIfFoundOnlyMaster;
    private boolean fineIfFoundOnlySlave;
    private boolean initialConnection;
    private boolean failoverLoop;

    public SearchFilter() {
        // empty constructor
    }

    /**
     * Constructor.
     *
     * @param fineIfFoundOnlyMaster stop searching if master found
     * @param fineIfFoundOnlySlave stop searching if slave found
     */
    public SearchFilter(boolean fineIfFoundOnlyMaster, boolean fineIfFoundOnlySlave) {
        this.fineIfFoundOnlyMaster = fineIfFoundOnlyMaster;
        this.fineIfFoundOnlySlave = fineIfFoundOnlySlave;
    }

    /**
     * Constructor.
     *
     * @param initialConnection initial connection flag
     */
    public SearchFilter(boolean initialConnection) {
        this.initialConnection = initialConnection;
    }

    public boolean isInitialConnection() {
        return initialConnection;
    }

    public boolean isFineIfFoundOnlyMaster() {
        return fineIfFoundOnlyMaster;
    }

    public boolean isFineIfFoundOnlySlave() {
        return fineIfFoundOnlySlave;
    }

    public boolean isFailoverLoop() {
        return failoverLoop;
    }

    public void setFailoverLoop(boolean failoverLoop) {
        this.failoverLoop = failoverLoop;
    }

    @Override
    public String toString() {
        return "SearchFilter{" + ", fineIfFoundOnlyMaster=" + fineIfFoundOnlyMaster
               + ", fineIfFoundOnlySlave=" + fineIfFoundOnlySlave + ", initialConnection="
               + initialConnection + ", failoverLoop=" + failoverLoop + "}";
    }
}
