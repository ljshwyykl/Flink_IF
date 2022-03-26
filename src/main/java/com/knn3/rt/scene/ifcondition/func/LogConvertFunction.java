package com.knn3.rt.scene.ifcondition.func;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.deserialization.CDCModel;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.util.Collector;

/**
 * @Author apophis
 * @File LogConvertFunction
 * @Time 2022/3/25 13:52
 * @Description 工程描述
 */
@Slf4j
public class LogConvertFunction implements FlatMapFunction<CDCModel, LogWrapper> {
    private final String appContract;

    public LogConvertFunction(String appContract) {
        this.appContract = appContract;
    }

    @Override
    public void flatMap(CDCModel cdcModel, Collector<LogWrapper> collector) throws Exception {
        if (!CDCModel.INSERT.equals(cdcModel.getOptType())) return;
        String afValue = cdcModel.getAfValue();
        if (afValue == null || afValue.length() == 0) return;
        LogWrapper logWrapper = Cons.MAPPER.readValue(afValue, new TypeReference<LogWrapper>() {
        });
        if (this.appContract.equals(logWrapper.getAddress())) collector.collect(logWrapper);
    }
}
