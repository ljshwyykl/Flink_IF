package com.knn3.rt.scene.ifcondition;

import com.knn3.rt.scene.ifcondition.deserialization.CDCModel;
import com.knn3.rt.scene.ifcondition.deserialization.CDCSchema;
import com.knn3.rt.scene.ifcondition.func.LogConvertFunction;
import com.knn3.rt.scene.ifcondition.func.TokenCalFunction;
import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import com.knn3.rt.scene.ifcondition.sink.KafkaSink;
import com.knn3.rt.scene.ifcondition.sink.RDBMSSink;
import com.knn3.rt.scene.ifcondition.sink.SinkModel;
import com.knn3.rt.scene.ifcondition.utils.JobUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

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


        Properties properties = new Properties();
        properties.setProperty("slot.drop.on.stop", paramMap.get("slot.drop.on.stop"));
        properties.setProperty("snapshot.select.statement.overrides", paramMap.get("snapshot.select.statement.overrides"));
        properties.setProperty(paramMap.get("snapshot.select.statement.overrides.key"), paramMap.get("snapshot.select.statement.overrides.value"));

        OutputTag<SinkModel> financeTag = new OutputTag<SinkModel>("finance-tag") {
        };

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

        String ifCondition = paramMap.get("app_if_fans_token_threshold_condition");
        SingleOutputStreamOperator<Balance[]> balanceStream = env.addSource(cdcSource)
                .flatMap(new LogConvertFunction(paramMap.get("app_contract")))
                .keyBy(LogWrapper::getAddress)
                .process(new TokenCalFunction(ifCondition, financeTag));
        // 写数据库
        balanceStream.addSink(new RDBMSSink(ifCondition));
        // 写kafka
        balanceStream.getSideOutput(financeTag).addSink(KafkaSink.sinkWithKey(paramMap.get("kafka_brokers"), paramMap.get("kafka_topic")));

        env.execute(jobName);
    }
}