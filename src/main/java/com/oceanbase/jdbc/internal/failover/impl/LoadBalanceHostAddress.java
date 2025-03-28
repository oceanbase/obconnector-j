package com.oceanbase.jdbc.internal.failover.impl;

import com.oceanbase.jdbc.HostAddress;

public class LoadBalanceHostAddress extends HostAddress {
    private int weight;

    public LoadBalanceHostAddress(String host, int port) {
        super(host, port);
    }

    public LoadBalanceHostAddress(String host, int port, int weight) {
        super(host, port);
        this.weight = weight;
    }

    public LoadBalanceHostAddress(String host, int port, String type) {
        super(host, port, type);
    }

    @Override
    public String toString() {
        return "LoadBalanceHostAddress{" + "weight=" + weight + ", host='" + host + '\''
               + ", port=" + port + ", type='" + type + '\'' + '}';
    }

    public String toJson() {
        StringBuilder json = new StringBuilder("\"HOST\":\"" + host + "\",\"PORT\":" + port);
        if (weight > 0) {
            json.append(",\"WEIGHT\":").append(weight);
        }
        return json.toString();
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

}
