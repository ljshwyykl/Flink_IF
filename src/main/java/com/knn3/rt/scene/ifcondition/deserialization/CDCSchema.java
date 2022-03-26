package com.knn3.rt.scene.ifcondition.deserialization;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @Author apophis
 * @File CDCSchema
 * @Time 2022/3/25 21:26
 * @Description 工程描述
 */
public class CDCSchema implements DebeziumDeserializationSchema<CDCModel> {
    private static final String AF = "after";
    private static final String BF = "before";
    private static final ObjectMapper MAPPER = Optional.of(
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    // 属性为NULL 不序列化
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    ).get();

    @Override
    public void deserialize(SourceRecord record, Collector<CDCModel> out) throws Exception {
        String topic = record.topic();
        Long timestamp = record.timestamp();
        Struct valueStruct = (Struct) record.value();
        Struct afStruct = valueStruct.getStruct(CDCSchema.AF);
        Struct bfStruct = valueStruct.getStruct(CDCSchema.BF);

        CDCModel model = new CDCModel();

        // 注意：若valueStruct中只有after,则表明插入；若只有before，说明删除；若既有before，也有after，则代表更新
        String optType = afStruct != null && bfStruct != null ? CDCModel.UPDATE : (afStruct != null ? CDCModel.INSERT : CDCModel.DELETE);
        switch (optType) {
            case CDCModel.INSERT:
                model.setAfValue(this.extractStruct(afStruct));
                break;
            case CDCModel.UPDATE:
                model.setAfValue(this.extractStruct(afStruct));
                model.setBfValue(this.extractStruct(bfStruct));
                break;
            case CDCModel.DELETE:
                model.setBfValue(this.extractStruct(bfStruct));
                break;
            default:
                throw new Exception("unknown opt");
        }
        model.setOptType(optType);
        model.setTopic(topic);
        model.setTimestamp(timestamp);
        out.collect(model);
    }

    @Override
    public TypeInformation<CDCModel> getProducedType() {
        return TypeInformation.of(CDCModel.class);
    }

    /**
     * 解析Struct 提取字段
     *
     * @param struct
     * @return
     */
    private String extractStruct(Struct struct) throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        for (Field field : struct.schema().fields()) {
            String fieldName = field.name();
            Object fieldValue = struct.get(fieldName);
            if (fieldName == null || fieldName.length() == 0 || fieldValue == null) continue;
            map.put(fieldName, fieldValue);
        }
        return map.isEmpty() ? null : CDCSchema.MAPPER.writeValueAsString(map);
    }
}
