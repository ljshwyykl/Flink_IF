package com.knn3.rt.scene.ifcondition.sink;

import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
import com.knn3.rt.scene.ifcondition.model.Status;
import com.knn3.rt.scene.ifcondition.utils.JDBCUtils;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class MySink extends RichSinkFunction<Balance[]> {
    private static final String DCP_TABLE = "S_Impossible_Finance";
    private final static String STATUS_INSERT = "INSERT INTO public.\"F_Status\"(id,address,dcp_table,uid) VALUES(?,?,?,?) ON CONFLICT DO NOTHING;";
    private final static String STATUS_DELETE = "DELETE FROM public.\"F_Status\" where dcp_table=? and address in(?);";
    private final static String FINANCE_INSERT = "INSERT INTO public.\"S_Impossible_Finance\"(id,chain_id,contract_id,token_symbol,token_name,address,campaign_id,campaign_name,block_number,if_fans_token_threshold,balance) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (address) DO UPDATE SET balance = ?;";
    private final static String FINANCE_DELETE = "DELETE FROM public.\"S_Impossible_Finance\" where address in(?);";
    private String ifCondition;
    private QueryRunner qr;
    private HikariDataSource ds;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String jdbcUrl = params.getRequired("pg_url");
        String userName = params.getRequired("pg_username");
        String password = params.getRequired("pg_password");
        JDBCUtils.loadDriver(JDBCUtils.POSTGRES);
        this.ds = JDBCUtils.getDataSource(jdbcUrl, userName, password);
        this.qr = new QueryRunner();
        this.ifCondition = params.get("app_if_fans_token_threshold_condition");
    }

    @Override
    public void close() throws Exception {
        if (this.ds != null) this.ds.close();
        this.ds = null;
        this.qr = null;
    }

    @Override
    public void invoke(Balance[] value, Context context) throws Exception {
        List<ImpossibleFinance> insertList = new ArrayList<>();
        List<Status> insertStatusList = new ArrayList<>();
        List<String> delList = new ArrayList<>();
        for (Balance balance : value)
            if (balance.getBalance().compareTo(new BigInteger(this.ifCondition)) >= 0) {
                ImpossibleFinance finance = this.ofFinanceMsg(balance);
                insertList.add(finance);
                insertStatusList.add(this.ofStatusMsg(balance, finance.getId()));
            } else delList.add(balance.getAddress());

        Connection connection = null;
        try {
            connection = this.ds.getConnection();
            // 不自动提交事务
            connection.setAutoCommit(false);
            // 操作数据库

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
                this.qr.batch(connection, MySink.STATUS_INSERT, args);
            }

            // 批量删除 F_Status
            if (delList.size() != 0)
                this.qr.update(connection, MySink.STATUS_DELETE, MySink.DCP_TABLE, String.format("'%s'", String.join("','", delList)));


            // 批量插入 S_Impossible_Finance
            if (insertList.size() != 0) {
                Object[][] args = new Object[insertList.size()][12];
                for (int i = 0; i < insertList.size(); i++) {
                    ImpossibleFinance finance = insertList.get(i);
                    args[i][0] = UUID.randomUUID();
                    args[i][1] = finance.getChainId();
                    args[i][2] = finance.getContractId();
                    args[i][3] = finance.getTokenSymbol();
                    args[i][4] = finance.getTokenName();
                    args[i][5] = finance.getAddress();
                    args[i][6] = finance.getCampaignId();
                    args[i][7] = finance.getCampaignName();
                    args[i][8] = finance.getBlockNumber();
                    args[i][9] = finance.getIfFansTokenThreshold();
                    args[i][10] = finance.getBalance();
                    args[i][11] = finance.getBalance();
                }
                this.qr.batch(connection, MySink.FINANCE_INSERT, args);
            }

            // 批量删除 S_Impossible_Finance
            if (delList.size() != 0)
                this.qr.update(connection, MySink.FINANCE_DELETE, String.format("'%s'", String.join("','", delList)));

            // 提交事务,关闭连接
            DbUtils.commitAndCloseQuietly(connection);
        } catch (SQLException e) {
            // 回滚,关闭连接
            DbUtils.rollbackAndCloseQuietly(connection);
            MySink.log.error("delList={},insertList={}", delList, insertList);
            MySink.log.error("发生异常,执行回滚", e);
        }
    }

    private Status ofStatusMsg(Balance balance, UUID id) {
        Status status = new Status();
        status.setId(UUID.randomUUID());
        status.setDcpTable(MySink.DCP_TABLE);
        status.setUid(id);
        status.setAddress(balance.getAddress());
        return status;
    }

    private ImpossibleFinance ofFinanceMsg(Balance balance) {
        String contractId = "0xB0e1fc65C1a741b4662B813eB787d369b8614Af1";

        ImpossibleFinance impossibleFinance = new ImpossibleFinance();
        impossibleFinance.setId(UUID.randomUUID());
        impossibleFinance.setAddress(balance.getAddress());
        impossibleFinance.setChainId("56");
        impossibleFinance.setContractId(contractId);
        impossibleFinance.setTokenSymbol("IF");
        impossibleFinance.setTokenName("Impossible Finance");
        impossibleFinance.setCampaignId(contractId);
        impossibleFinance.setCampaignName(MySink.DCP_TABLE);
        impossibleFinance.setBlockNumber(balance.getBlockNumber());
        impossibleFinance.setBalance(balance.getBalance());
        impossibleFinance.setIfFansTokenThreshold(true);
        return impossibleFinance;
    }
}

