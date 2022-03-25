package com.knn3.rt.scene.ifcondition.func;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * @Author apophis
 * @File LogConvertFunction
 * @Time 2022/3/25 13:52
 * @Description 工程描述
 */
public class LogConvertFunction implements FlatMapFunction<String, LogWrapper> {
    private final String appContract;

    public LogConvertFunction(String appContract) {
        this.appContract = appContract;
    }

    @Override
    public void flatMap(String value, Collector<LogWrapper> collector) throws Exception {
        if (StringUtils.isNullOrWhitespaceOnly(value)) return;
        LogWrapper logWrapper = Cons.MAPPER.readValue(value, new TypeReference<LogWrapper>() {
        });
        if (this.appContract.equals(logWrapper.getAddress())) collector.collect(logWrapper);
    }
}
