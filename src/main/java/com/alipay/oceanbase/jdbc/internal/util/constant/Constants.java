package com.alipay.oceanbase.jdbc.internal.util.constant;

/**
 * Represents various constants used in the driver.
 */
public class Constants {
    /**
     * Avoids allocation of empty byte[] when representing 0-length strings.
     */
    public final static byte[] EMPTY_BYTE_ARRAY                = new byte[0];

    /**
     * I18N'd representation of the abbreviation for "ms"
     */
    public final static String MILLIS_I18N                     = Messages.getString("Milliseconds");

    public final static byte[] SLASH_STAR_SPACE_AS_BYTES       = new byte[] { (byte) '/',
            (byte) '*', (byte) ' '                            };

    public final static byte[] SPACE_STAR_SLASH_SPACE_AS_BYTES = new byte[] { (byte) ' ',
            (byte) '*', (byte) '/', (byte) ' '                };

    /**
     * Prevents instantiation
     */
    private Constants() {
    }
}
