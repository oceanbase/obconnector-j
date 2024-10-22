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
package com.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TimezoneExplicitCalendarTest extends BaseTest {

    private static final TimeZone EUROPE_PARIS = TimeZone.getTimeZone("Europe/Paris");

    private TimeZone              previousTimeZone;

    // Just to avoid the tests passing by chance when the JVM timezone is the same as the explicit
    // timezone passed in tests
    @Before
    public void setDefaultTimeZoneToGmt() {
        previousTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @After
    public void restorePreviousDefaultTimeZone() {
        TimeZone.setDefault(previousTimeZone);
    }

    @Test
  public void testDateWithExplicitTimeZone() throws SQLException {
    try (Connection connection = setConnectionWithEuropeParisTimeZone()) {
      Date epochInGmt = new Date(0);
      PreparedStatement st = connection.prepareStatement("SELECT ?");
      Calendar writeCalendar = Calendar.getInstance(EUROPE_PARIS);
      st.setDate(1, epochInGmt, writeCalendar);

      ResultSet rs = st.executeQuery();
      assertTrue(rs.next());
      Calendar readCalendar = Calendar.getInstance(EUROPE_PARIS);
      assertEquals(rs.getDate(1, readCalendar), epochInGmt);
    }
  }

    @Test
  public void testTimestampWithExplicitTimeZone() throws SQLException {
    try (Connection connection = setConnectionWithEuropeParisTimeZone()) {
      Timestamp epochInGmt = new Timestamp(0);
      PreparedStatement st = connection.prepareStatement("SELECT ?");
      Calendar writeCalendar = Calendar.getInstance(EUROPE_PARIS);
      st.setTimestamp(1, epochInGmt, writeCalendar);

      ResultSet rs = st.executeQuery();
      assertTrue(rs.next());
      Calendar readCalendar = Calendar.getInstance(EUROPE_PARIS);
      assertEquals(rs.getTimestamp(1, readCalendar), epochInGmt);
    }
  }

    @Test
  public void testTimeWithExplicitTimeZone() throws SQLException {
    try (Connection connection = setConnectionWithEuropeParisTimeZone()) {
      Time midnightInGmt = new Time(0);
      PreparedStatement st = connection.prepareStatement("SELECT ?");
      Calendar writeCalendar = Calendar.getInstance(EUROPE_PARIS);
      st.setTime(1, midnightInGmt, writeCalendar);

      ResultSet rs = st.executeQuery();
      assertTrue(rs.next());
      Calendar readCalendar = Calendar.getInstance(EUROPE_PARIS);
      assertEquals(rs.getTime(1, readCalendar), midnightInGmt);
    }
  }

    private Connection setConnectionWithEuropeParisTimeZone() throws SQLException {
        return setConnection("&serverTimezone=Europe/Paris");
    }
}
