package org.knn3.flink;

import com.knn3.rt.scene.ifcondition.func.LogConvertFunction;
import com.knn3.rt.scene.ifcondition.func.TokenCalFunction;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import com.knn3.rt.scene.ifcondition.utils.JobUtils;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @Author apophis
 * @File IfConditionJob
 * @Time 2022/3/25 12:15
 * @Description 工程描述
 */
public class TestIfConditionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv(args);
        String jobName = TestIfConditionJob.class.getSimpleName();
        Map<String, String> paramMap = env.getConfig().getGlobalJobParameters().toMap();

        String appContract = paramMap.get("app_contract");

        DebeziumSourceFunction<String> cdcSource = PostgreSQLSource.<String>builder()
                .hostname(paramMap.get("pg_hostname"))
                .port(Integer.parseInt(paramMap.get("pg_port")))
                .database(paramMap.get("pg_database"))
                .schemaList(paramMap.get("pg_schemaList"))
                .tableList(paramMap.get("pg_tableList"))
                .username(paramMap.get("pg_username"))
                .password(paramMap.get("pg_password"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.addSource(cdcSource)
                .flatMap(new LogConvertFunction(appContract))
                .keyBy(LogWrapper::getAddress)
                .process(new TokenCalFunction())
                .print();

        env.execute(jobName);
    }
}
