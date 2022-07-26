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
package com.oceanbase.jdbc.internal.com;

/**
 * Creates result packets only handles error, ok, eof and result set packets since field and row
 * packets require a previous result set stream.
 */
public class Packet {
    public static final byte ERROR                        = (byte) 0xff;
    public static final byte OK                           = (byte) 0x00;
    public static final byte EOF                          = (byte) 0xfe;
    public static final byte LOCAL_INFILE                 = (byte) 0xfb;

    // send command
    public static final byte COM_QUIT                     = (byte) 0x01;
    public static final byte COM_INIT_DB                  = (byte) 0x02;
    public static final byte COM_QUERY                    = (byte) 0x03;
    public static final byte COM_PING                     = (byte) 0x0e;
    public static final byte COM_STMT_PREPARE             = (byte) 0x16;
    public static final byte COM_STMT_EXECUTE             = (byte) 0x17;
    public static final byte COM_STMT_PREPARE_EXECUTE     = (byte) 0xa1; // for oracle mode
    public static final byte COM_STMT_FETCH               = (byte) 0x1c;
    public static final byte COM_STMT_SEND_LONG_DATA      = (byte) 0x18;
    public static final byte COM_STMT_CLOSE               = (byte) 0x19;
    public static final byte COM_RESET_CONNECTION         = (byte) 0x1f;
    public static final byte COM_STMT_BULK_EXECUTE        = (byte) 0xfa;
    public static final byte COM_MULTI                    = (byte) 0xfe;
    public static final byte COM_CHANGE_USER              = (byte) 0x11;
    public static final byte COM_STMT_SEND_PIECE_DATA     = (byte) 0xa2;
    public static final byte COM_STMT_GET_PIECE_DATA      = (byte) 0xa3;

    // prepare statement cursor flag.
    public static final byte CURSOR_TYPE_NO_CURSOR        = (byte) 0x00;
    public static final byte CURSOR_TYPE_READ_ONLY        = (byte) 0x01;
    public static final byte CURSOR_TYPE_FOR_UPDATE       = (byte) 0x02;
    public static final byte CURSOR_TYPE_SCROLLABLE       = (byte) 0x04; // reserved, but not implemented server side

    /*----------------------- Execution Modes -----------------------------------*/
    public static final int  OCI_BATCH_MODE               = 0x00000001; // batch the oci stmt for exec
    public static final int  OCI_EXACT_FETCH              = 0x00000002; // fetch exact rows specified
    //public static final int                               = 0x00000004; // available
    public static final int  OCI_STMT_SCROLLABLE_READONLY = 0x00000008; //if result set is scrollable
    public static final int  OCI_DESCRIBE_ONLY            = 0x00000010; // only describe the statement
    public static final int  OCI_COMMIT_ON_SUCCESS        = 0x00000020; // commit, if successful exec
    public static final int  OCI_NON_BLOCKING             = 0x00000040; // non-blocking
    public static final int  OCI_BATCH_ERRORS             = 0x00000080; // batch errors in array dmls
    public static final int  OCI_PARSE_ONLY               = 0x00000100; // only parse the statement
    public static final int  OCI_EXACT_FETCH_RESERVED_1   = 0x00000200; // reserved
    public static final int  OCI_SHOW_DML_WARNINGS        = 0x00000400; // return OCI_SUCCESS_WITH_INFO for delete/update w/no where clause
    public static final int  OCI_EXEC_RESERVED_2          = 0x00000800; // reserved
    public static final int  OCI_DESC_RESERVED_1          = 0x00001000; // reserved
    public static final int  OCI_EXEC_RESERVED_3          = 0x00002000; // reserved
    public static final int  OCI_EXEC_RESERVED_4          = 0x00004000; // reserved
    public static final int  OCI_EXEC_RESERVED_5          = 0x00008000; // reserved
    public static final int  OCI_EXEC_RESERVED_6          = 0x00010000; // reserved
    public static final int  OCI_RESULT_CACHE             = 0x00020000; // hint to use query caching
    public static final int  OCI_NO_RESULT_CACHE          = 0x00040000; // hint to bypass query caching
    public static final int  OCI_EXEC_RESERVED_7          = 0x00080000; // reserved
    public static final int  OCI_RETURN_ROW_COUNT_ARRAY   = 0x00100000; // Per Iter DML Row Count mode
    /*---------------------------------------------------------------------------*/

    public static final byte OCI_ONE_PIECE                = (byte) 0x00;
    public static final byte OCI_FIRST_PIECE              = (byte) 0x01;
    public static final byte OCI_NEXT_PIECE               = (byte) 0x02;
    public static final byte OCI_LAST_PIECE               = (byte) 0x03;

    /*------------------------(Scrollable Cursor) Fetch Options-------------------
     * For non-scrollable cursor, the only valid (and default) orientation is OCI_FETCH_NEXT
     */
    public static final byte OCI_FETCH_CURRENT            = (byte) 0x01;
    public static final byte OCI_FETCH_NEXT               = (byte) 0x02;
    public static final byte OCI_FETCH_FIRST              = (byte) 0x04;
    public static final byte OCI_FETCH_LAST               = (byte) 0x08;
    public static final byte OCI_FETCH_PRIOR              = (byte) 0x10;
    public static final byte OCI_FETCH_ABSOLUTE           = (byte) 0x20;
    public static final byte OCI_FETCH_RELATIVE           = (byte) 0x40;
    /*---------------------------------------------------------------------------*/
    /*----------------------- Oci ArrayBinding Flag -----------------------------*/
    public static final byte OCI_ARRAY_BINDING            = (byte) 0x08;
    public static final byte OCI_SAVE_EXCEPTION           = (byte) 0x10;

}
