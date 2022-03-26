package com.knn3.rt.scene.ifcondition.sink;

import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
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
    private final static String INSERT = "INSERT INTO public.\"F_Status\"(id,address,dcp_table,uid) VALUES(?,?,?,?) ON CONFLICT DO NOTHING;";
    private final static String DELETE = "DELETE FROM public.\"F_Status\" where dcp_table='S_Impossible_Finance' and address in(?);";
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
        List<String> delList = new ArrayList<>();
        for (Balance balance : value)
            if (balance.getBalance().compareTo(new BigInteger(this.ifCondition)) >= 0)
                insertList.add(this.ofMsg(balance));
            else delList.add(balance.getAddress());


        Connection connection = null;
        try {
            connection = this.ds.getConnection();
            // 不自动提交事务
            connection.setAutoCommit(false);
            // 操作数据库

            // 批量插入
            if (insertList.size() != 0) {
                Object[][] args = new Object[insertList.size()][4];
                for (int i = 0; i < insertList.size(); i++) {
                    ImpossibleFinance finance = insertList.get(i);
                    args[i][0] = UUID.randomUUID();
                    args[i][1] = finance.getAddress();
                    args[i][2] = "S_Impossible_Finance";
                    args[i][3] = UUID.randomUUID();
                }
                this.qr.batch(connection, MySink.INSERT, args);
            }

            if (delList.size() != 0) this.qr.update(MySink.DELETE, String.format("'%s'", String.join("','", delList)));

            // 提交事务,关闭连接
            DbUtils.commitAndCloseQuietly(connection);
            MySink.log.error("提交事务");
        } catch (SQLException e) {
            // 回滚,关闭连接
            DbUtils.rollbackAndCloseQuietly(connection);
            e.printStackTrace();
            MySink.log.error("取消事务,回滚");
        }
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

