package com.knn3.rt.scene.ifcondition.deserialization;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyDescription;

/**
 * @Author apophis
 * @File CDCModel
 * @Time 2022/3/25 21:25
 * @Description 工程描述
 */
@Setter
@Getter
@ToString
public class CDCModel {
    public static final String INSERT = "INSERT";
    public static final String DELETE = "DELETE";
    public static final String UPDATE = "UPDATE";

    private String topic;
    private Long timestamp;
    @JsonPropertyDescription("变更类型")
    private String optType;
    @JsonPropertyDescription("变更前数据")
    private String bfValue;
    @JsonPropertyDescription("变更后数据")
    private String afValue;

}
