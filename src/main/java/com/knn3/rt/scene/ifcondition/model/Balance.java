package com.knn3.rt.scene.ifcondition.model;

import lombok.*;

import java.math.BigInteger;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Balance {
    private String token;
    private String address;
    private BigInteger balance;
    private Integer blockNumber;
}