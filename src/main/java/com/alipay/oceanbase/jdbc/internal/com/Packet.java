package com.alipay.oceanbase.jdbc.internal.com;

/**
 * Creates result packets only handles error, ok, eof and result set packets since field and row
 * packets require a previous result set stream.
 */
public class Packet {

    public static final byte ERROR                   = (byte) 0xff;
    public static final byte OK                      = (byte) 0x00;
    public static final byte EOF                     = (byte) 0xfe;
    public static final byte LOCAL_INFILE            = (byte) 0xfb;

    // send command
    public static final byte COM_QUIT                = (byte) 0x01;
    public static final byte COM_INIT_DB             = (byte) 0x02;
    public static final byte COM_QUERY               = (byte) 0x03;
    public static final byte COM_PING                = (byte) 0x0e;
    public static final byte COM_STMT_PREPARE        = (byte) 0x16;
    public static final byte COM_STMT_EXECUTE        = (byte) 0x17;
    public static final byte COM_STMT_FETCH          = (byte) 0x1c;
    public static final byte COM_STMT_SEND_LONG_DATA = (byte) 0x18;
    public static final byte COM_STMT_CLOSE          = (byte) 0x19;
    public static final byte COM_RESET_CONNECTION    = (byte) 0x1f;
    public static final byte COM_STMT_BULK_EXECUTE   = (byte) 0xfa;
    public static final byte COM_MULTI               = (byte) 0xfe;

    // prepare statement cursor flag.
    public static final byte CURSOR_TYPE_NO_CURSOR   = (byte) 0x00;
    public static final byte CURSOR_TYPE_READ_ONLY   = (byte) 0x01;
    public static final byte CURSOR_TYPE_FOR_UPDATE  = (byte) 0x02;
    public static final byte CURSOR_TYPE_SCROLLABLE  = (byte) 0x04; // reserved, but not implemented server side
}
