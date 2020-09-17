/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
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

package org.mariadb.jdbc.internal.com.send.parameters;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import org.mariadb.jdbc.extend.datatype.TIMESTAMP;
import org.mariadb.jdbc.internal.*;
import org.mariadb.jdbc.internal.io.output.*;

public class OBTIMESTAMPParameter implements Cloneable, ParameterHolder {

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private final Timestamp ts;
  private final TimeZone timeZone;
  private final boolean fractionalSeconds;

  private static final byte[] LITERALS_TIMESTAMP = "timestamp ".getBytes();
  private static final int ORACLE_TIME_SCALE = 9; // 序列化oracle时间类型时，固定填充scale 9

  /**
   * Constructor.
   *
   * @param ts timestamps
   * @param timeZone timeZone
   * @param fractionalSeconds must fractional Seconds be send to database.
   */
  public OBTIMESTAMPParameter(Timestamp ts, TimeZone timeZone, boolean fractionalSeconds) {
    this.ts = ts;
    this.timeZone = timeZone;
    this.fractionalSeconds = fractionalSeconds;
  }

  /**
   * Write timestamps to outputStream.
   *
   * @param pos the stream to write to
   */
  public void writeTo(final PacketOutputStream pos) throws IOException {
    sdf.setTimeZone(timeZone);

    // oracle literals
    // timestamp '2020-01-01 00:00:00.000'
    pos.write(LITERALS_TIMESTAMP);
    pos.write(QUOTE);
    pos.write(sdf.format(ts).getBytes());
    int microseconds = ts.getNanos() / 1000;
    if (microseconds > 0 && fractionalSeconds) {
      pos.write('.');
      int factor = 100000;
      while (microseconds > 0) {
        int dig = microseconds / factor;
        pos.write('0' + dig);
        microseconds -= dig * factor;
        factor /= 10;
      }
    }
    pos.write(QUOTE);
  }

  public int getApproximateTextProtocolLength() {
    return 27;
  }

  /**
   * Write data to socket in binary format.
   *
   * @param pos socket output stream
   * @throws IOException if socket error occur
   */
  public void writeBinary(final PacketOutputStream pos) throws IOException {
    TIMESTAMP timestamp = new TIMESTAMP(ts);
    byte[] data;
    data = timestamp.getBytes();
    pos.write((byte) data.length);
    pos.write(data, 0, 11);
    pos.write((byte) ORACLE_TIME_SCALE);
  }

  public ColumnType getColumnType() {
    return ColumnType.TIMESTAMP_NANO;
  }

  @Override
  public String toString() {
    return "'" + ts.toString() + "'";
  }

  public boolean isNullData() {
    return false;
  }

  public boolean isLongData() {
    return false;
  }
}
