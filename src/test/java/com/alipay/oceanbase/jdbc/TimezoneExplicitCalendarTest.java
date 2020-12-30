package com.alipay.oceanbase.jdbc;

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
