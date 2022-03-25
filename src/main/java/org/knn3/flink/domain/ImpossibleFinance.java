package org.knn3.flink.domain;


import lombok.Data;

import java.math.BigInteger;


@Data
public class ImpossibleFinance extends StatusPublicField {
    private Boolean ifFansTokenThreshold;
    private BigInteger balance;
    public ImpossibleFinance() {}

}
