package com.knn3.rt.scene.ifcondition.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

@Setter
@Getter
@ToString
public class StatusPublicField {
    private UUID id;
    private String chainId;
    private String contractId;
    private String tokenId;
    private String tokenSymbol;
    private String tokenUri;
    private String tokenName;
    private String address;
    private String campaignId;
    private String campaignName;
    private Integer blockNumber;
}
