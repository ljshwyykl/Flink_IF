package org.knn3.flink.domain;

import lombok.Data;

import java.math.BigInteger;

@Data
public class Balance {
    private String token;
    private String address;
    private BigInteger balance;
    private Integer blockNumber;

    public Balance() {
    }

    public Balance(String token, String address, BigInteger balance,Integer blockNumber) {
        this.token = token;
        this.address = address;
        this.balance = balance;
        this.blockNumber = blockNumber;
    }
}