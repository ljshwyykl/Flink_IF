package com.knn3.rt.scene.ifcondition;

import com.alibaba.fastjson.JSONObject;
import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.deserialization.CustomDebeziumDeserializationSchema;
import com.knn3.rt.scene.ifcondition.func.LogConvertFunction;
import com.knn3.rt.scene.ifcondition.func.TokenCalFunction;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import com.knn3.rt.scene.ifcondition.sink.MySink;
import com.knn3.rt.scene.ifcondition.sink.SinkPG;
import com.knn3.rt.scene.ifcondition.utils.JobUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;

/**
 * @Author apophis
 * @File IfConditionJob
 * @Time 2022/3/25 12:15
 * @Description 工程描述
 */
public class IfConditionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv(args);
        String jobName = IfConditionJob.class.getSimpleName();
        Map<String, String> paramMap = env.getConfig().getGlobalJobParameters().toMap();

        String appContract = paramMap.get("app_contract");

        Properties properties = new Properties();
        properties.setProperty("plugin.name", paramMap.get("debezium_plugin.name"));
        properties.setProperty("slot.drop.on.stop", paramMap.get("debezium_slot.drop.on.stop"));
        properties.setProperty("snapshot.select.statement.overrides", paramMap.get("debezium_snapshot.select.statement.overrides"));
        properties.setProperty(paramMap.get("debezium_snapshot.select.statement.overrides.key"), paramMap.get("debezium_snapshot.select.statement.overrides.value"));


        DebeziumSourceFunction<JSONObject> cdcSource = PostgreSQLSource.<JSONObject>builder()
                .hostname(paramMap.get("pg_hostname"))
                .port(Integer.parseInt(paramMap.get("pg_port")))
                .database(paramMap.get("pg_database")) // monitor postgres database
                .schemaList(paramMap.get("pg_schemaList"))  // monitor inventory schema
                .tableList(paramMap.get("pg_tableList")) // monitor products table
                .username(paramMap.get("pg_username"))
                .password(paramMap.get("pg_password"))
                .debeziumProperties(properties)
                .deserializer(new CustomDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .slotName(paramMap.get("pg_slotName"))
                .build();

        env.addSource(cdcSource)
                .map(x-> Cons.MAPPER.writeValueAsString(x))
                .flatMap(new LogConvertFunction(appContract))
                .keyBy(LogWrapper::getAddress)
               .process(new TokenCalFunction())
              // .addSink(new MySink());
       .print();

        env.execute(jobName);
    }
}


// com.knn3.rt.scene.ifcondition.IfConditionJob
//--mode cluster --properties /Users/wei/Desktop/Flink_IF/src/main/resources/application.properties  --checkpointUrl file:/Users/wei/Desktop/project/flink/flink-checkpoints/if --backend fs