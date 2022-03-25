package org.knn3.flink;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.func.LogConvertFunction;
import com.knn3.rt.scene.ifcondition.func.TokenCalFunction;
import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import com.knn3.rt.scene.ifcondition.utils.JobUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @Author apophis
 * @File IfConditionJob
 * @Time 2022/3/25 12:15
 * @Description 工程描述
 */
public class TestLocalIfConditionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JobUtils.getEnv(args);
        String jobName = TestLocalIfConditionJob.class.getSimpleName();
        Map<String, String> paramMap = env.getConfig().getGlobalJobParameters().toMap();

        String appContract = "0xB0e1fc65C1a741b4662B813eB787d369b8614Af1";


        env.fromElements("{\"blockHash\":\"0x909c270e64f206370bf42af29322b9d853d7d9d2138eaa2e2b647d1624f6cd42\",\"address\":\"0xB0e1fc65C1a741b4662B813eB787d369b8614Af1\",\"logIndex\":1074,\"data\":\"0x0000000000000000000000000000000000000000000000000de0b6b3a7640000\",\"removed\":false,\"topics\":[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x000000000000000000000000f1f52f33abab0629ec8657c330ad556824b844e1\",\"0x0000000000000000000000002669a7565899ab4bd7b7642aa2725e751de6035c\"],\"blockNumber\":9482069,\"transactionIndex\":390,\"transactionHash\":\"0x2ca02ca0e632a852980a1058db0dd11ff8c55430b31f773723ef630467417cfd\"}",
                        "{\"blockHash\":\"0x321434f20ee5649d4511a1adcf4fcf1c7ecd4dedb6057f37908252a30aa6c1c9\",\"address\":\"0xB0e1fc65C1a741b4662B813eB787d369b8614Af1\",\"logIndex\":552,\"data\":\"0x000000000000000000000000000000000000000000000067ed959cce4b638012\",\"removed\":false,\"topics\":[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x000000000000000000000000f1f52f33abab0629ec8657c330ad556824b844e1\",\"0x000000000000000000000000a344896f950a8378974132f7e5e2b310afb6d50a\"],\"blockNumber\":9485008,\"transactionIndex\":273,\"transactionHash\":\"0xe3c4bfec174123dc5cec3f3db49b790ae1ad0d025879cbafa141763cafb315c6\"}",
                        "{\"blockHash\":\"0x1a8e6371c527f7fb41c95262e4d6b51f7424830bffc9798518b50e49eec44b0c\",\"address\":\"0xB0e1fc65C1a741b4662B813eB787d369b8614Af1\",\"logIndex\":874,\"data\":\"0x000000000000000000000000000000000000000000000011350b078e9d932724\",\"removed\":false,\"topics\":[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x00000000000000000000000068a37e14d080436b57119e379a04d32f4ca7f92d\",\"0x000000000000000000000000a344896f950a8378974132f7e5e2b310afb6d50a\"],\"blockNumber\":9485065,\"transactionIndex\":365,\"transactionHash\":\"0xbfac7b84c1d54f142775e887abec22eef492988a6083271efdaaa8f1c77623ae\"}",
                        "{\"blockHash\":\"0xf5da4a426747a45e7f71c4af32557d5987a41b152cd60b10d1d04d3a5a7eac26\",\"address\":\"0xB0e1fc65C1a741b4662B813eB787d369b8614Af1\",\"logIndex\":160,\"data\":\"0x0000000000000000000000000000000000000000000000028e697a33b3e55899\",\"removed\":false,\"topics\":[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x0000000000000000000000006eb0569afb79ec893c8212cbf4dbad74eea666aa\",\"0x00000000000000000000000068a37e14d080436b57119e379a04d32f4ca7f92d\"],\"blockNumber\":9485336,\"transactionIndex\":51,\"transactionHash\":\"0x062e6c26b9a912a9761dad7544e101f9f60cce7e157d8fa8362ce964a939df9c\"}")
                .flatMap(new LogConvertFunction(appContract))
                .keyBy(LogWrapper::getAddress)
                .process(new TokenCalFunction())
                .flatMap(new FlatMapFunction<Balance[], String>() {
                    @Override
                    public void flatMap(Balance[] balances, Collector<String> collector) throws Exception {
                        for (Balance balance : balances) collector.collect(Cons.MAPPER.writeValueAsString(balance));
                    }
                })
                .print();

        env.execute(jobName);
    }
}
