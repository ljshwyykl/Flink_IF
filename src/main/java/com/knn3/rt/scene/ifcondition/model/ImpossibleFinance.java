package com.knn3.rt.scene.ifcondition.model;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyDescription;

import java.math.BigInteger;


@Setter
@Getter
@ToString
public class ImpossibleFinance extends StatusPublicField {
    public static final String INSERT = "INSERT";
    public static final String DELETE = "DELETE";
    @JsonPropertyDescription("若DELETE,则为false")
    private Boolean ifFansTokenThreshold;
    private BigInteger balance;
    @JsonPropertyDescription("INSERT或DELETE")
    private String flag;
}
