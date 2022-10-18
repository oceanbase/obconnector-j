package com.oceanbase.jdbc.internal.failover.impl;

import com.oceanbase.jdbc.HostAddress;

public class LoadBalanceHostAddress extends HostAddress {
    private int weight;

    @Override
    public String toString() {
        return "LoadBalanceHostAddress{" + "weight=" + weight + ", host='" + host + '\''
               + ", port=" + port + ", type='" + type + '\'' + '}';
    }

    public LoadBalanceHostAddress(String host, int port) {
        super(host, port);
    }

    public LoadBalanceHostAddress(String host, int port, String type) {
        super(host, port, type);
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

}
