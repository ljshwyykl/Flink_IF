package org.knn3.flink.sink;

/*

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ibatis.session.SqlSession;
import org.knn3.flink.Bootstrap;
import org.knn3.flink.domain.Balance;
import org.knn3.flink.domain.ImpossibleFinance;
import org.knn3.flink.domain.Status;
import org.knn3.flink.utils.MyBatisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class SinkPG1 extends RichSinkFunction<Balance[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkPG.class);


    private SqlSession session;

    private String ifCondition;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameter = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();


        this.session = MyBatisUtils.openSession();
        this.ifCondition = parameter.get("app_if_fans_token_threshold_condition");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(this.session!= null){
            this.session.close();
            MyBatisUtils.closeSession(this.session);
        }

    }

    @Override
    public void invoke(Balance[] value, Context context) throws Exception {
        // S_Impossible_Finance
        List insertList = new ArrayList();
        List delList = new ArrayList();

        // S_Status
        List insertStatusList = new ArrayList();

        // 遍历数据集合
        for (Balance balance : value) {
            if (balance.getBalance().compareTo(new BigInteger(this.ifCondition)) >= 0) {
                UUID id = UUID.randomUUID();


                ImpossibleFinance impossibleFinance = new ImpossibleFinance();
                impossibleFinance.setId(id);
                impossibleFinance.setAddress(balance.getAddress());
                impossibleFinance.setChainId("56");
                impossibleFinance.setContractId("0xB0e1fc65C1a741b4662B813eB787d369b8614Af1");
                impossibleFinance.setTokenSymbol("IF");
                impossibleFinance.setTokenName("Impossible Finance");
                impossibleFinance.setCampaignId("0xB0e1fc65C1a741b4662B813eB787d369b8614Af1");
                impossibleFinance.setCampaignName("S_Impossible_Finance");
                impossibleFinance.setBlockNumber(balance.getBlockNumber());
                impossibleFinance.setBalance(balance.getBalance());
                impossibleFinance.setIfFansTokenThreshold(true);

                insertList.add(impossibleFinance);

                Status status = new Status();
                status.setDcpTable("S_Impossible_Finance");
                status.setUid(id.toString());
                status.setAddress(balance.getAddress());
                insertStatusList.add(status);
            } else {
                delList.add(balance.getAddress());
            }

        }


        LOGGER.info("insertList size {}", insertList.size());
        LOGGER.info("delList size {}", delList.size());

        if (insertList.size() > 0) {
            session.insert("impossibleFinance.batchInsert", insertList);
            session.insert("status.batchInsert", insertStatusList);
        }
        if (delList.size() > 0) {
            session.delete("impossibleFinance.batchDelete", delList);
            session.delete("status.batchDelete", delList);
        }

        session.commit();//提交事务数据
    }
}


*/
