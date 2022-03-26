package com.knn3.rt.scene.ifcondition;

import com.knn3.rt.scene.ifcondition.deserialization.CDCModel;
import com.knn3.rt.scene.ifcondition.deserialization.CDCSchema;
import com.knn3.rt.scene.ifcondition.func.LogConvertFunction;
import com.knn3.rt.scene.ifcondition.func.TokenCalFunction;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import com.knn3.rt.scene.ifcondition.sink.MySink;
import com.knn3.rt.scene.ifcondition.utils.JobUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
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
        env.setParallelism(1);

        String appContract = paramMap.get("app_contract");

        Properties properties = new Properties();
        properties.setProperty("slot.drop.on.stop", paramMap.get("slot.drop.on.stop"));
        properties.setProperty("snapshot.select.statement.overrides", paramMap.get("snapshot.select.statement.overrides"));
        properties.setProperty(paramMap.get("snapshot.select.statement.overrides.key"), paramMap.get("snapshot.select.statement.overrides.value"));


        DebeziumSourceFunction<CDCModel> cdcSource = PostgreSQLSource.<CDCModel>builder()
                .hostname(paramMap.get("pg_hostname"))
                .port(Integer.parseInt(paramMap.get("pg_port")))
                .database(paramMap.get("pg_database"))
                .schemaList(paramMap.get("pg_schemaList"))
                .tableList(paramMap.get("pg_tableList"))
                .username(paramMap.get("pg_username"))
                .password(paramMap.get("pg_password"))
                .debeziumProperties(properties)
                .deserializer(new CDCSchema())
                .slotName(paramMap.get("pg_slotName"))
                .decodingPluginName(paramMap.get("plugin.name"))
                .build();

        env.addSource(cdcSource)
                .flatMap(new LogConvertFunction(appContract))
                .keyBy(LogWrapper::getAddress)
                .process(new TokenCalFunction())
                .addSink(new MySink());
        // .map(Cons.MAPPER::writeValueAsString)
        // .print();

        env.execute(jobName);
    }
}