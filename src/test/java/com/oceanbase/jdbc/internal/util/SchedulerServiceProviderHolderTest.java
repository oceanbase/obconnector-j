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
package com.oceanbase.jdbc.internal.util;

import static org.junit.Assert.*;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.oceanbase.jdbc.internal.util.scheduler.DynamicSizedSchedulerInterface;
import com.oceanbase.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

public class SchedulerServiceProviderHolderTest {

    @After
    @Before
    public void providerReset() {
        SchedulerServiceProviderHolder.setSchedulerProvider(null);
    }

    @Test
    public void getDefaultProviderTest() {
        assertTrue(SchedulerServiceProviderHolder.DEFAULT_PROVIDER == SchedulerServiceProviderHolder
            .getSchedulerProvider());
    }

    @Test
    public void defaultProviderGetSchedulerTest() {
        testRunnable(SchedulerServiceProviderHolder.getScheduler(1, "testScheduler", 8));
        testRunnable(SchedulerServiceProviderHolder.getFixedSizeScheduler(1, "testFixedScheduler"));
    }

    private void testRunnable(ScheduledExecutorService scheduler) {
    try {
      assertNotNull(scheduler);
      // verify scheduler works
      scheduler.execute(
          () -> {
            try {
              Thread.sleep(10);
            } catch (InterruptedException ie) {
              // eat
            }
          });
      scheduler.shutdown();
      Assert.assertTrue(scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS));
    } catch (InterruptedException ie) {
      fail();
    }
  }

    @Test
    public void defaultProviderSchedulerShutdownTest() {
        testExecuteAfterShutdown(SchedulerServiceProviderHolder.getScheduler(1, "testScheduler", 8));
        testExecuteAfterShutdown(SchedulerServiceProviderHolder.getFixedSizeScheduler(1,
            "testFixedScheduler"));
    }

    private void testExecuteAfterShutdown(ScheduledExecutorService scheduler) {
    scheduler.shutdown();
    try {
      scheduler.execute(() -> {});
      fail("Exception should have thrown");
    } catch (RejectedExecutionException expected) {
      // ignore
    }
  }

    @Test
    public void setAndGetProviderTest() {
        SchedulerServiceProviderHolder.SchedulerProvider emptyProvider = new SchedulerServiceProviderHolder.SchedulerProvider() {
            @Override
            public DynamicSizedSchedulerInterface getScheduler(int minimumThreads, String poolName,
                                                               int maximumPoolSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getFixedSizeScheduler(int minimumThreads,
                                                                     String poolName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getTimeoutScheduler() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getBulkScheduler() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };

        SchedulerServiceProviderHolder.setSchedulerProvider(emptyProvider);
        assertTrue(emptyProvider == SchedulerServiceProviderHolder.getSchedulerProvider());
    }
}
