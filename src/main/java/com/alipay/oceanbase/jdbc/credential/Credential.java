package com.alipay.oceanbase.jdbc.credential;

public class Credential {
    private String user;
    private String password;

    public Credential(String user, String password) {
        this.user = user;
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }
}
