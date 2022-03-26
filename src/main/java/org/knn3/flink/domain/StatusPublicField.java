package org.knn3.flink.domain;

import lombok.Data;

@Data
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

    public StatusPublicField() {
    }
}
