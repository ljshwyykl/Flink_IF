package com.knn3.rt.scene.ifcondition.constant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * @Author apophis
 * @File Cons
 * @Time 2022/3/25 13:53
 * @Description 工程描述
 */
public class Cons {
    public static final ObjectMapper MAPPER = Optional.of(
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    // 属性为NULL 不序列化
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    ).get();
}
