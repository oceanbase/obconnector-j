package com.alipay.oceanbase.jdbc.credential.env;

import com.alipay.oceanbase.jdbc.HostAddress;
import com.alipay.oceanbase.jdbc.credential.Credential;
import com.alipay.oceanbase.jdbc.credential.CredentialPlugin;
import com.alipay.oceanbase.jdbc.util.Options;

/**
 * Authentication using environment variable.
 *
 * <p>default implementation use environment variable MARIADB_USER and MARIADB_PWD
 *
 * <p>example : `jdbc:oceanbase://host/db?credentialType=ENV`
 *
 * <p>2 options `userKey` and `pwdKey` permits to indicate which environment variable to use.
 */
public class EnvCredentialPlugin implements CredentialPlugin {

    private Options options;
    private String  userName;

    @Override
    public String type() {
        return "ENV";
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
        String envUser = System.getenv(userKey != null ? userKey : "MARIADB_USER");
        return new Credential(envUser == null ? userName : envUser,
            System.getenv(pwdKey != null ? pwdKey : "MARIADB_PWD"));
    }
}
