package com.knn3.rt.scene.ifcondition.sink;

import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import com.knn3.rt.scene.ifcondition.utils.MyBatisUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.Reader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SinkPG extends RichSinkFunction<Balance[]> {
    private SqlSession session;
    private String ifCondition;
    Reader reader = null;
    private  SqlSessionFactory sqlSessionFactory = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        reader = Resources.getResourceAsReader("mybatis-config.xml");
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);

        ParameterTool parameter = (ParameterTool)
                this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // this.session = MyBatisUtils.openSession();
        this.ifCondition = parameter.get("app_if_fans_token_threshold_condition");
    }

    @Override
    public void close() throws Exception {
        super.close();
        // MyBatisUtils.closeSession(this.session);
    }


    @Override
    public void invoke(Balance[] value, Context context) throws Exception {
        SqlSession sqlSession = sqlSessionFactory.openSession(false);;

        List<ImpossibleFinance> insertList = new ArrayList<>();
        List<String> delList = new ArrayList<>();
        for (Balance balance : value)
            if (balance.getBalance().compareTo(new BigInteger(this.ifCondition)) >= 0)
                insertList.add(this.ofMsg(balance));
            else delList.add(balance.getAddress());


        SinkPG.log.info("insertList size {}", insertList.size());
        SinkPG.log.info("delList size {}", delList.size());

        if (insertList.size() > 0) this.session.insert("impossibleFinance.batchInsert", insertList);
        if (delList.size() > 0) this.session.delete("impossibleFinance.batchDelete", delList);


        sqlSession.commit();//提交事务数据
    }

    private ImpossibleFinance ofMsg(Balance balance) {
        ImpossibleFinance impossibleFinance = new ImpossibleFinance();
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
        return impossibleFinance;
    }
}
