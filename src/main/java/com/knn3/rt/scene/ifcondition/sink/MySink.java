package com.knn3.rt.scene.ifcondition.sink;

import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
import com.knn3.rt.scene.ifcondition.utils.JDBCUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class MySink extends RichSinkFunction<Balance[]> {
    private final static String INSERT = "INSERT INTO public.F_Status(id,address,dcp_table,uid) VALUES(?,?,?,?);";
    private final static String DELETE = "DELETE FROM public.F_Status where dcp_table='S_Impossible_Finance' and address in(?);";
    private String ifCondition;
    private QueryRunner qr;

    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        System.out.println(String.format("'%s'", String.join("','", list)));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//        String jdbcUrl = params.getRequired("jdbc:postgresql://localhost:5432/tr-notification?&amp;ssl=false");
//        String userName = params.getRequired("postgres");
//        String password = params.getRequired("postgres");

        String jdbcUrl ="jdbc:postgresql://localhost:5432/tr-notification?&amp;ssl=false";
        String userName = "postgres";
        String password = "postgres";
        this.qr = new QueryRunner(JDBCUtils.getDataSource(jdbcUrl, userName, password));
        this.ifCondition = params.get("app_if_fans_token_threshold_condition");
    }

    @Override
    public void invoke(Balance[] value, Context context) throws Exception {
        List<ImpossibleFinance> insertList = new ArrayList<>();
        List<String> delList = new ArrayList<>();
        for (Balance balance : value)
            if (balance.getBalance().compareTo(new BigInteger(this.ifCondition)) >= 0)
                insertList.add(this.ofMsg(balance));
            else delList.add(balance.getAddress());


        MySink.log.info("insertList size {}", insertList.size());
        MySink.log.info("delList size {}", delList.size());

        insertList.forEach(x -> {
            try {
                this.qr.update(MySink.INSERT, UUID.randomUUID(), x.getAddress(), "table", "uid");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        if (delList.size() > 0) this.qr.update(MySink.DELETE, String.format("'%s'", String.join("','", delList)));
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

