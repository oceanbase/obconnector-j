package com.alipay.oceanbase.jdbc.credential.system;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.credential.Credential;
import com.alipay.oceanbase.jdbc.credential.CredentialPlugin;
import com.alipay.oceanbase.jdbc.util.Options;

/**
 * Authentication using java system properties.
 *
 * <p>default implementation use system properties `mariadb.user` and `mariadb.pwd`
 *
 * <p>example : `jdbc:oceanbase://host/db?identityType=PROPERTY` will use environment variable
 * MARIADB_USER and MARIADB_PWD
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
 */
/**
 * Authentication using java system properties.
 *
 * <p>default implementation use system properties `mariadb.user` and `mariadb.pwd`
 *
 * <p>example : `jdbc:oceanbase://host/db?credentialType=PROPERTY`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which system properties to use .
 */
public class PropertiesCredentialPlugin implements CredentialPlugin {

    private Options options;
    private String  userName;

    @Override
    public String type() {
        return "PROPERTY";
    }

    @Override
    public String name() {
        return "Environment password";
    }

    @Override
    public CredentialPlugin initialize(Options options, String userName, HostAddress hostAddress) {
        this.options = options;
        this.userName = userName;
        return this;
    }

    @Override
    public Credential get() {

        String userKey = this.options.nonMappedOptions.getProperty("userKey");
        String pwdKey = this.options.nonMappedOptions.getProperty("pwdKey");
        String propUser = System.getProperty(userKey != null ? userKey : "mariadb.user");
        return new Credential(propUser == null ? userName : propUser,
            System.getProperty(pwdKey != null ? pwdKey : "mariadb.pwd"));
    }
}
