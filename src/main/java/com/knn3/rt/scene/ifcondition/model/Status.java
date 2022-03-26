package com.knn3.rt.scene.ifcondition.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyDescription;

import java.util.UUID;

/**
 * @Author apophis
 * @File Status
 * @Time 2022/3/26 18:56
 * @Description 工程描述
 */
@Setter
@Getter
@ToString
public class Status {
    private UUID id;
    private String address;
    private String dcpTable;
    @JsonPropertyDescription("ImpossibleFinance的id")
    private UUID uid;
}
