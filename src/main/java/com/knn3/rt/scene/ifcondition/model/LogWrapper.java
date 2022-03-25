package com.knn3.rt.scene.ifcondition.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

/**
 * @Author apophis
 * @File LogWrapper
 * @Time 2022/3/25 13:47
 * @Description 工程描述
 */
@Setter
@Getter
@ToString
public class LogWrapper {
    private String address;
    private Integer logIndex;
    private String transactionHash;
    private List<String> topics;
    private String data;
    private Integer blockNumber;
}
