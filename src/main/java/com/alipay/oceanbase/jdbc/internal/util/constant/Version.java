package com.alipay.oceanbase.jdbc.internal.util.constant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Version {
    public static final String version;
    public static final int    majorVersion;
    public static final int    minorVersion;
    public static final int    patchVersion;
    public static final String qualifier;

    static {
        InputStream inputStream = null;
        String tmpVersion = "2.6.2";
        try {
            Properties prop = new Properties();
            inputStream = Version.class.getResourceAsStream("/oceanbase.properties");
            if (inputStream != null) {
                prop.load(inputStream);
                // get the property value and print it out
                tmpVersion = prop.getProperty("version");
            } else {
                System.out
                    .println("property file 'oceanbase.properties' not found in the classpath");
            }
        } catch (Exception e) {
            // eat
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException ioe) {
                // eat
            }
        }
        version = tmpVersion;
        int major = 0;
        int minor = 0;
        int patch = 0;
        String qualif = "";

        int length = version.length();
        char car;
        int offset = 0;
        int type = 0;
        int val = 0;
        for (; offset < length; offset++) {
            car = version.charAt(offset);
            if (car < '0' || car > '9') {
                switch (type) {
                    case 0:
                        major = val;
                        break;
                    case 1:
                        minor = val;
                        break;
                    case 2:
                        patch = val;
                        qualif = version.substring(offset);
                        offset = length;
                        break;
                    default:
                        break;
                }
                type++;
                val = 0;
            } else {
                val = val * 10 + car - 48;
            }
        }

        if (type == 2) {
            patch = val;
        }
        majorVersion = major;
        minorVersion = minor;
        patchVersion = patch;
        qualifier = qualif;
    }
}
