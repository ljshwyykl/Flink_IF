package com.knn3.rt.scene.ifcondition.sink;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.model.Balance;
import com.knn3.rt.scene.ifcondition.model.ImpossibleFinance;
import com.knn3.rt.scene.ifcondition.model.Status;
import com.knn3.rt.scene.ifcondition.service.TransService;
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
public class RDBMSSink extends RichSinkFunction<Balance[]> {
    private final String ifCondition;
    private QueryRunner qr;
    private HikariDataSource ds;

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
        this.ds = JDBCUtils.getDataSource(jdbcUrl, userName, password);
        this.qr = new QueryRunner();
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
                ImpossibleFinance finance = TransService.ofFinanceMsg(balance);
                insertList.add(finance);
                insertStatusList.add(TransService.ofStatusMsg(balance, finance.getId()));
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
                this.qr.batch(connection, Cons.STATUS_INSERT, args);
            }

            // 批量删除 F_Status
            if (delList.size() != 0)
                this.qr.update(connection, Cons.STATUS_DELETE, Cons.DCP_TABLE, String.format("'%s'", String.join("','", delList)));


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
                this.qr.batch(connection, Cons.FINANCE_INSERT, args);
            }

            // 批量删除 S_Impossible_Finance
            if (delList.size() != 0)
                this.qr.update(connection, Cons.FINANCE_DELETE, String.format("'%s'", String.join("','", delList)));

            // 提交事务,关闭连接
            DbUtils.commitAndCloseQuietly(connection);
        } catch (SQLException e) {
            // 回滚,关闭连接
            DbUtils.rollbackAndCloseQuietly(connection);
            RDBMSSink.log.error("delList={},insertList={},insertStatusList={}", delList, insertList, insertStatusList);
            RDBMSSink.log.error("发生异常,执行回滚", e);
        }
    }
}
