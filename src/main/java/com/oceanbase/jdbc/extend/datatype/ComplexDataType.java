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
package com.oceanbase.jdbc.extend.datatype;

import java.sql.SQLException;

public class ComplexDataType {
    private String            typeName;
    private String            schemaName;
    private long              version         = 0;
    boolean                   isValid         = false;
    private int               type;
    private int               attrCount;
    private int               initAttrCount   = 0;
    private ComplexDataType[] attrTypes       = null;

    public static final int   TYPE_NUMBER     = 0;
    public static final int   TYPE_VARCHAR2   = 1;
    public static final int   TYPE_DATE       = 2;
    public static final int   TYPE_OBJECT     = 3;
    public static final int   TYPE_COLLECTION = 4;
    public static final int   TYPE_CURSOR     = 5;
    public static final int   TYPE_RAW        = 6;
    public static final int   TYPE_CHAR       = 7;
    public static final int   TYPE_TIMESTMAP  = 8;
    public static final int   TYPE_CLOB       = 9;
    public static final int   TYPE_MAX        = 10;

    public ComplexDataType(String typeName, String schemaName, int type) {
        this.schemaName = schemaName;
        this.type = type;
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getAttrCount() {
        return attrCount;
    }

    public void setAttrCount(int attrCount) {
        this.attrCount = attrCount;
    }

    public ComplexDataType[] getAttrTypes() {
        return this.attrTypes;
    }

    public ComplexDataType getAttrType(int attrIndex) {
        if (attrIndex >= this.attrCount) {
            return null;
        }
        return this.attrTypes[attrIndex];
    }

    public void setAttrType(int attrIndex, ComplexDataType attrType) {
        if (null == this.attrTypes) {
            this.attrTypes = new ComplexDataType[this.attrCount];
        }
        this.attrTypes[attrIndex] = attrType;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public int getInitAttrCount() {
        return initAttrCount;
    }

    public void incInitAttrCount() {
        ++this.initAttrCount;
    }

    public static int getObComplexType(String attrType) throws SQLException {
        if (attrType == null) {
            return TYPE_MAX;
        }
        if (attrType.equalsIgnoreCase("COLLECTION")) {
            return TYPE_COLLECTION;
        }
        if (attrType.equalsIgnoreCase("OBJECT")) {
            return TYPE_OBJECT;
        }
        if (attrType.equalsIgnoreCase("NUMBER") || attrType.equalsIgnoreCase("INTEGER")
            || attrType.equalsIgnoreCase("DECIMAL")) {
            return TYPE_NUMBER;
        }
        if (attrType.equalsIgnoreCase("VARCHAR2") || attrType.equalsIgnoreCase("VARCHAR")) {
            return TYPE_VARCHAR2;
        }
        if (attrType.equalsIgnoreCase("DATE")) {
            return TYPE_DATE;
        }
        if (attrType.equalsIgnoreCase("CURSOR")) {
            return TYPE_CURSOR;
        }
        if (attrType.equalsIgnoreCase("RAW")) {
            return TYPE_RAW;
        }
        if (attrType.equalsIgnoreCase("CHAR")) {
            return TYPE_CHAR;
        }
        if (attrType.toUpperCase().indexOf("TIMESTAMP") != -1) {
            return TYPE_TIMESTMAP;
        }
        if (attrType.equalsIgnoreCase("CLOB")) {
            return TYPE_CLOB;
        }
        return TYPE_MAX;
    }

    public static boolean isBaseDataType(int type) {
        return TYPE_NUMBER == type || TYPE_VARCHAR2 == type || TYPE_DATE == type;
    }

    public static byte[] getBytes() {
        return null;
    }
}
