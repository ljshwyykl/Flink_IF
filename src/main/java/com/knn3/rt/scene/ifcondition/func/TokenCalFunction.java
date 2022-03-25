package com.knn3.rt.scene.ifcondition.func;

import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.LogWrapper;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.web3j.utils.Numeric;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/**
 * @Author apophis
 * @File TokenCalFunction
 * @Time 2022/3/25 14:35
 * @Description 工程描述
 */
public class TokenCalFunction extends KeyedProcessFunction<String, LogWrapper, Balance[]> {
    //之前的操作记录
    private transient MapState<String, Integer> repeatMapState;
    private transient MapState<String, BigInteger> balanceMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.repeatMapState = this.getRuntimeContext().getMapState(new MapStateDescriptor<>("repeat-state", String.class, Integer.class));
        this.balanceMapState = this.getRuntimeContext().getMapState(new MapStateDescriptor<>("balanceMapState-state", String.class, BigInteger.class));
    }

    @Override
    public void processElement(LogWrapper logWrapper, KeyedProcessFunction<String, LogWrapper, Balance[]>.Context context, Collector<Balance[]> collector) throws Exception {
        Integer logIndex = logWrapper.getLogIndex();
        String transactionHash = logWrapper.getTransactionHash();
        String key = String.format("%s:%s", transactionHash, logIndex);
        // 仅计算首次
        if (this.repeatMapState.get(key) != null) return;
        this.repeatMapState.put(key, 1);

        List<String> topics = logWrapper.getTopics();
        String from = "0x" + topics.get(1).substring(26);
        String to = "0x" + topics.get(2).substring(26);
        String token = logWrapper.getAddress();
        BigInteger value = Numeric.toBigInt(logWrapper.getData());
        Integer blockNumber = logWrapper.getBlockNumber();

        Balance[] arr = new Balance[2];

        BigInteger bfFromValue = Optional.ofNullable(this.balanceMapState.get(from)).orElseGet(() -> new BigInteger("0")).subtract(value);
        arr[0] = new Balance(token, from, bfFromValue, blockNumber);

        BigInteger bfToValue = Optional.ofNullable(this.balanceMapState.get(to)).map(x -> x.add(value)).orElseGet(() -> new BigInteger("" + value));
        arr[1] = new Balance(token, from, bfToValue, blockNumber);

        this.balanceMapState.put(from, bfFromValue);
        this.balanceMapState.put(to, bfFromValue);

        collector.collect(arr);
    }
}
