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
package com.oceanbase.jdbc.Benchmark;

import org.junit.Test;
import org.openjdk.jmh.annotations.*;

import com.oceanbase.jdbc.internal.util.Utils;

@Warmup(iterations = 3)
public class ApiBenchmark {
    @State(Scope.Benchmark)
    public static class ExecutionPlan {
        @Param({
                "insert /* woshilize */ into t values( 'wo',?,:name) /* c43 */ -- sfs \n",
                "SELECT\n" + "  -- Packaged procedures with no arguments\n"
                        + "  package_name AS procedure_cat,\n" + "  owner AS procedure_schem,\n"
                        + "  object_name AS procedure_name,\n" + "  NULL,\n" + "  NULL,\n"
                        + "  NULL,\n" + "  'Packaged procedure' AS remarks,\n"
                        + "  1 AS procedure_type\n" + ",  NULL AS specific_name\n"
                        + "FROM all_arguments\n" + "WHERE argument_name IS NULL\n"
                        + "  AND data_type IS NULL\n" + "  AND package_name LIKE ? \n"
                        + "  AND owner LIKE ? \n" + "  AND object_name LIKE ? \n"
                        + "UNION ALL SELECT\n" + "  -- Packaged procedures with arguments\n"
                        + "  package_name AS procedure_cat,\n" + "  owner AS procedure_schem,\n"
                        + "  object_name AS procedure_name,\n" + "  NULL,\n" + "  NULL,\n"
                        + "  NULL,\n" + "  'Packaged procedure' AS remarks,\n"
                        + "  1 AS procedure_type\n" + ",  NULL AS specific_name\n"
                        + "FROM all_arguments" })
        public String  sqlString;
        public boolean noBackslashEscapes;
        @Param({ "true", "false" })
        public boolean isOracleMode;
        public boolean skipComment;
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void trimSqlStringTest(ExecutionPlan plan) {
        Utils.trimSQLString(plan.sqlString, plan.noBackslashEscapes, plan.isOracleMode,
            plan.skipComment);

    }

    @Test
    public void benchmark() throws Exception {
        String[] argv = {};
        org.openjdk.jmh.Main.main(argv);
    }

}
