package com.knn3.rt.scene.ifcondition.model;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigInteger;


@Setter
@Getter
@ToString
public class ImpossibleFinance extends StatusPublicField {
    private Boolean ifFansTokenThreshold;
    private BigInteger balance;
}
