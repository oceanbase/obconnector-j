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

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.oceanbase.jdbc.Driver;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.util.constant.HaMode;
import com.oceanbase.jdbc.util.DefaultOptions;
import com.oceanbase.jdbc.util.Options;

public class DefaultOptionsTest {

    @Test
    public void parseDefault() throws Exception {
        // check that default option object correspond to default
        Options option = new Options();
        DefaultOptions.postOptionProcess(option, null);
        for (HaMode haMode : HaMode.values()) {
            Options defaultOption = DefaultOptions.parse(haMode, "", new Properties(), null);
            DefaultOptions.postOptionProcess(defaultOption, null);
            for (DefaultOptions o : DefaultOptions.values()) {
                Field field = Options.class.getField(o.getOptionName());
                assertEquals("field :" + field.getName(),
                    field.get(DefaultOptions.defaultValues(HaMode.NONE)), field.get(option));
                assertEquals("field :" + field.getName(),
                    field.get(DefaultOptions.defaultValues(haMode)), field.get(defaultOption));
            }
        }
    }

    @Test
    public void getDefaultPropertyInfo() throws Exception {
        // check that default option object correspond to default
        Driver driver = new Driver();
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo(
            "jdbc:oceanbase://localhost:3306/testj?user=hi", new Properties());
        assertTrue(driverPropertyInfos.length > 30);
        Assert.assertEquals(driverPropertyInfos[0].name, "user");
        Assert.assertEquals(driverPropertyInfos[0].value, "hi");
        Assert.assertEquals(driverPropertyInfos[0].description, "Database user name");
    }

    @Test
    public void getDefaultPropertyInfoNoUrl() throws Exception {
        // check that default option object correspond to default
        Driver driver = new Driver();
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo(null, new Properties());
        assertTrue(driverPropertyInfos.length > 30);
        Assert.assertEquals(driverPropertyInfos[0].name, "user");
        Assert.assertEquals(driverPropertyInfos[0].value, null);
        Assert.assertEquals(driverPropertyInfos[0].description, "Database user name");
    }

    @Test
    public void getPropertyInfoNoUrl() throws Exception {
        // check that default option object correspond to default
        Driver driver = new Driver();
        Properties properties = new Properties();
        properties.put("user", "t2");
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo(null, properties);
        assertTrue(driverPropertyInfos.length > 30);
        Assert.assertEquals(driverPropertyInfos[0].name, "user");
        Assert.assertEquals(driverPropertyInfos[0].value, "t2");
        Assert.assertEquals(driverPropertyInfos[0].description, "Database user name");
    }

    @Test
  public void parseDefaultDriverManagerTimeout() throws Exception {
    DriverManager.setLoginTimeout(2);
    UrlParser parser = UrlParser.parse("jdbc:oceanbase://localhost/");
    assertEquals(parser.getOptions().connectTimeout, 2_000);

    DriverManager.setLoginTimeout(0);

    parser = UrlParser.parse("jdbc:oceanbase://localhost/");
    assertEquals(parser.getOptions().connectTimeout, 30_000);
  }

    /**
     * Ensure that default value of new Options() correspond to DefaultOption Enumeration.
     *
     * @throws Exception if any value differ.
     */
    @Test
    public void parseOption() throws Exception {
        Options option = new Options();
        String param = generateParam();
        for (HaMode haMode : HaMode.values()) {
            Options resultOptions = DefaultOptions.parse(haMode, param, new Properties(), null);
            for (Field field : Options.class.getFields()) {
                // Because of security vulnerabilities now mandatory cannot be set, but may be supported in the future
                if (field.getName().equals("allowLoadLocalInfile")) {
                    continue;
                }
                if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())
                    && !"nonMappedOptions".equals(field.getName())) {
                    switch (field.getType().getName()) {
                        case "java.lang.String":
                            assertEquals("field " + field.getName() + " value error for param"
                                         + param, field.get(resultOptions), field.getName() + "1");
                            break;
                        case "int":
                            assertEquals("field " + field.getName() + " value error for param"
                                         + param, field.getInt(resultOptions), 9999);
                            break;
                        case "java.lang.Integer":
                            assertEquals("field " + field.getName() + " value error for param"
                                         + param, ((Integer) field.get(resultOptions)).intValue(),
                                9999);
                            break;
                        case "java.lang.Long":
                            assertEquals("field " + field.getName() + " value error for param"
                                         + param, ((Long) field.get(resultOptions)).intValue(),
                                9999);
                            break;
                        case "java.lang.Boolean":
                            Boolean bool = (Boolean) field.get(option);
                            if (bool == null) {
                                assertTrue("field " + field.getName() + " value error for param"
                                           + param, (Boolean) field.get(resultOptions));
                            } else {
                                assertEquals("field " + field.getName() + " value error for param"
                                             + param, field.get(resultOptions), !bool);
                            }
                            break;
                        case "boolean":
                            assertEquals("field " + field.getName() + " value error for param"
                                         + param, field.getBoolean(resultOptions),
                                !field.getBoolean(option));
                            break;
                        default:
                            fail("type not used normally ! " + field.getType().getName());
                    }
                }
            }
        }
    }

    private String generateParam() throws IllegalAccessException {
        Options option = new Options();
        // check option url settings
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (DefaultOptions defaultOption : DefaultOptions.values()) {
            for (Field field : Options.class.getFields()) {
                if (defaultOption.getOptionName().equals(field.getName())) { // for having same order
                    if (first) {
                        first = false;
                    } else {
                        sb.append("&");
                    }
                    sb.append(field.getName()).append("=");
                    switch (field.getType().getName()) {
                        case "java.lang.String":
                            sb.append(field.getName()).append("1");
                            break;
                        case "int":
                        case "java.lang.Integer":
                        case "java.lang.Long":
                            sb.append("9999");
                            break;
                        case "java.lang.Boolean":
                            Boolean bool = (Boolean) field.get(option);
                            if (bool == null) {
                                sb.append("true");
                            } else {
                                sb.append((!((Boolean) field.get(option))));
                            }
                            break;
                        case "boolean":
                            sb.append(!(boolean) (field.get(option)));
                            break;
                        default:
                            fail("type not used normally ! : " + field.getType().getName());
                    }
                }
            }
        }
        return sb.toString();
    }

    @Test
    public void buildTest() throws Exception {
        String param = generateParam();
        for (HaMode haMode : HaMode.values()) {
            Options resultOptions = DefaultOptions.parse(haMode, param, new Properties(), null);
            StringBuilder sb = new StringBuilder();
            DefaultOptions.propertyString(resultOptions, haMode, sb);

            assertEquals("?" + param, sb.toString());
        }
    }
}
