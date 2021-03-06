package org.knn3.flink;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.knn3.flink.deserialization.CustomDebeziumDeserializationSchema;
import org.knn3.flink.domain.Balance;
import org.knn3.flink.sink.SinkPG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.utils.Numeric;

import java.io.InputStream;
import java.math.BigInteger;
import java.util.Properties;

public class Bootstrap {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);


    public static void main(String[] args) throws Exception {
        Bootstrap.LOGGER.info("Bootstrap main");

        final String fileName = "application.properties";
        // System.out.println("main");
        InputStream inputStream = Bootstrap.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameter = ParameterTool.fromPropertiesFile(inputStream);

        Properties properties = new Properties();
        properties.setProperty("plugin.name", parameter.get("debezium_plugin.name"));
        properties.setProperty("slot.drop.on.stop", parameter.get("debezium_slot.drop.on.stop"));
        properties.setProperty("snapshot.select.statement.overrides", parameter.get("debezium_snapshot.select.statement.overrides"));
        properties.setProperty(parameter.get("debezium_snapshot.select.statement.overrides.key"), parameter.get("debezium_snapshot.select.statement.overrides.value"));

        SourceFunction<JSONObject> sourceFunction = PostgreSQLSource.<JSONObject>builder()
                .hostname(parameter.get("pg_hostname"))
                .port(parameter.getInt("pg_port"))
                .database(parameter.get("pg_database")) // monitor postgres database
                .schemaList(parameter.get("pg_schemaList"))  // monitor inventory schema
                .tableList(parameter.get("pg_tableList")) // monitor products table
                .deserializer(new CustomDebeziumDeserializationSchema()) // converts ?? to JSON String
                .username(parameter.get("pg_username"))
                .password(parameter.get("pg_password"))
                .debeziumProperties(properties)
                // .slotName(parameter.get("pg_slotName"))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameter);


        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // ??????????????????????????????????????????????????????????????????????????????5????????????????????????10s???
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000));
        // ??????????????? CheckPoint ?????????????????????????????????????????????????????????????????????????????????????????? CheckPoint ?????????????????????
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        env.addSource(sourceFunction)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        // LOGGER.info("jsonObject:{}", jsonObject);
//                        LOGGER.info("equals:{}", jsonObject.get("address").equals(parameter.get("app_contract")));

                        return !jsonObject.isEmpty() && jsonObject.get("address").equals(parameter.get("app_contract"));
                    }
                })
                .keyBy(value -> value.getString("address"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //?????????????????????
                    private transient MapState<String, Boolean> mapState;


                    /***???????????????*/
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        super.open(parameters);
                        MapStateDescriptor descriptor = new MapStateDescriptor("RepeatMapState", String.class, Boolean.class);
                        this.mapState = this.getRuntimeContext().getMapState(descriptor);

                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        //
                        // ?????? logIndex transactionHash
                        Integer logIndex = jsonObject.getInteger("logIndex");
                        String transactionHash = jsonObject.getString("transactionHash");

                        String key = transactionHash + ":" + logIndex.toString();

                        Bootstrap.LOGGER.info("key:{}", key);
                        Boolean state = this.mapState.get(key);

                        if (state == null) {
                            this.mapState.put(key, true);
                            collector.collect(jsonObject);
                        } else Bootstrap.LOGGER.info("id exist::{}", key);
                    }
                })
                .keyBy(value -> value.getString("address"))
                .map(new RichMapFunction<JSONObject, Balance[]>() {

                    private transient MapState<String, BigInteger> mapState;

                    /***???????????????*/
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor descriptor = new MapStateDescriptor("MapStateBalance", String.class, BigInteger.class);
                        this.mapState = this.getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public Balance[] map(JSONObject jsonObject) throws Exception {
                        // System.out.println("map:topics:" + jsonObject.getJSONArray("topics").get(0));

                        String from = "0x" + jsonObject.getJSONArray("topics").get(1).toString().substring(26);
                        String to = "0x" + jsonObject.getJSONArray("topics").get(2).toString().substring(26);
//                        System.out.println("map:from:" + from);
//                        System.out.println("map:to:" + to);


                        String token = jsonObject.getString("address");
//                        System.out.println("map:token:" + token);

                        BigInteger value = Numeric.toBigInt((jsonObject.getString("data")));
//                        System.out.println("map:value:" + value);
                        Bootstrap.LOGGER.info("map:from:{}", from);
                        Bootstrap.LOGGER.info("map:to:{}", to);
                        Bootstrap.LOGGER.info("map:value:{}", value);

                        BigInteger beforeFrom = this.mapState.get(from);

                        Balance[] balanceArr = new Balance[2];

                        BigInteger calValue;
                        if (beforeFrom == null) calValue = new BigInteger("0").subtract(value);
                        else
                            calValue = beforeFrom.subtract(value);
                        this.mapState.put(from, calValue);
                        balanceArr[0] = new Balance(token, from, calValue, jsonObject.getInteger("blockNumber"));


                        BigInteger beforeTo = this.mapState.get(to);
                        if (beforeTo == null) calValue = new BigInteger(String.valueOf(value));
                        else
                            calValue = beforeTo.add(value);

                        this.mapState.put(to, calValue);
                        balanceArr[1] = new Balance(token, to, calValue, jsonObject.getInteger("blockNumber"));


                        return balanceArr;
                    }
                })
                .addSink(new SinkPG());
        //.print();

        env.execute("Impossible Finance Loyalty System");
    }


}
