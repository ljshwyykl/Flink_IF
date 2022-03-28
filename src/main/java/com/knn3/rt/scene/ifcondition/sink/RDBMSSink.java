package com.knn3.rt.scene.ifcondition.sink;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
import com.knn3.rt.scene.ifcondition.model.Status;
import com.knn3.rt.scene.ifcondition.service.TransService;
import com.knn3.rt.scene.ifcondition.utils.JDBCUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RDBMSSink extends RichSinkFunction<Balance[]> {
    private final String ifCondition;
    private QueryRunner qr;

    public RDBMSSink(String ifCondition) {
        this.ifCondition = ifCondition;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String jdbcUrl = params.getRequired("pg_url");
        String userName = params.getRequired("pg_username");
        String password = params.getRequired("pg_password");
        JDBCUtils.loadDriver(JDBCUtils.POSTGRES);
        this.qr = new QueryRunner(JDBCUtils.getDataSource(jdbcUrl, userName, password));
    }

    @Override
    public void invoke(Balance[] value, Context context) throws Exception {
        List<ImpossibleFinance> insertList = new ArrayList<>();
        List<Status> insertStatusList = new ArrayList<>();
        List<String> delList = new ArrayList<>();
        for (Balance balance : value)
            if (balance.getBalance().compareTo(new BigInteger(this.ifCondition)) >= 0) {
                ImpossibleFinance finance = TransService.ofFinanceMsg(balance);
                insertList.add(finance);
                insertStatusList.add(TransService.ofStatusMsg(balance, finance.getId()));
            } else delList.add(balance.getAddress());


        // 批量插入 F_Status
        if (insertStatusList.size() != 0) {
            Object[][] args = new Object[insertStatusList.size()][4];
            for (int i = 0; i < insertStatusList.size(); i++) {
                Status status = insertStatusList.get(i);
                args[i][0] = UUID.randomUUID();
                args[i][1] = status.getAddress();
                args[i][2] = status.getDcpTable();
                args[i][3] = status.getUid();
            }
            this.qr.batch(Cons.STATUS_INSERT, args);
        }

        // 批量插入 S_Impossible_Finance
        if (insertList.size() != 0) for (ImpossibleFinance finance : insertList) {
            Object[] args = new Object[13];
            args[0] = UUID.randomUUID();
            args[1] = finance.getChainId();
            args[2] = finance.getContractId();
            args[3] = finance.getTokenSymbol();
            args[4] = finance.getTokenName();
            args[5] = finance.getAddress();
            args[6] = finance.getCampaignId();
            args[7] = finance.getCampaignName();
            args[8] = finance.getBlockNumber();
            args[9] = finance.getIfFansTokenThreshold();
            args[10] = finance.getBalance();
            args[11] = finance.getBalance();
            args[12] = finance.getBlockNumber();
            this.qr.update(Cons.FINANCE_INSERT, args);
        }

        if (delList.size() != 0) {
            Object[] arr = delList.toArray();
            // 批量删除 F_Status
            String delStatusSql = String.format(Cons.STATUS_DELETE, Cons.DCP_TABLE, IntStream.range(0, delList.size()).mapToObj(x -> "?").collect(Collectors.joining(",")));
            this.qr.update(delStatusSql, arr);
            // 批量删除 S_Impossible_Finance
            String delFinanceSql = String.format(Cons.FINANCE_DELETE, IntStream.range(0, delList.size()).mapToObj(x -> "?").collect(Collectors.joining(",")));
            this.qr.update(delFinanceSql, arr);
        }
    }
}

