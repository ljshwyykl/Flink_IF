package com.knn3.rt.scene.ifcondition.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class StatusPublicField {
    private String id;
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
