package com.knn3.rt.scene.ifcondition.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author
 * @ClassName JDBCUtils
 * @date 2020/5/23
 * @desc 无
 */
public class JDBCUtils {
    private final static String MYSQL_DRIVER = "org.postgresql.Driver";
    //创建连接池,只创建一次,所以使用static final
    private static HikariDataSource ds = null;

    //获取连接池
    public static HikariDataSource getDataSource(String jdbcUrl, String userName, String passWord) throws PropertyVetoException {
        if (JDBCUtils.ds == null) {
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(JDBCUtils.MYSQL_DRIVER);
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(userName);
            config.setPassword(passWord);
            config.addDataSourceProperty("cachePrepStmts", "true"); //是否自定义配置，为true时下面两个参数才生效
            config.addDataSourceProperty("prepStmtCacheSize", "250"); //连接池大小默认25，官方推荐250-500
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048"); //单条语句最大长度默认256，官方推荐2048
            config.addDataSourceProperty("useServerPrepStmts", "true"); //新版本MySQL支持服务器端准备，开启能够得到显著性能提升
            config.addDataSourceProperty("useLocalSessionState", "true");
            config.addDataSourceProperty("useLocalTransactionState", "true");
            config.addDataSourceProperty("rewriteBatchedStatements", "true");
            config.addDataSourceProperty("cacheResultSetMetadata", "true");
            config.addDataSourceProperty("cacheServerConfiguration", "true");
            config.addDataSourceProperty("elideSetAutoCommits", "true");
            config.addDataSourceProperty("maintainTimeStats", "false");
            config.setConnectionTestQuery("select 1");
            config.setAutoCommit(true);
            // config.setReadOnly(true);
            config.setConnectionTimeout(30000);
            config.setIdleTimeout(30000);
            config.setMaxLifetime(1800000);
            config.setMaximumPoolSize(10);
            JDBCUtils.ds = new HikariDataSource(config);
        }
        return JDBCUtils.ds;
    }

    //连接数据库
    public static Connection getConnection() {
        Connection con = null;
        try {
            con = JDBCUtils.ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }

    public static void release(Connection con, PreparedStatement ps, ResultSet rs) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            con = null;
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ps = null;
        }
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            rs = null;
        }
    }

    public static void release(Connection con, PreparedStatement ps) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            con = null;
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ps = null;
        }
    }

    public static void main(String[] args) throws PropertyVetoException, SQLException {
        String jdbcUrl = "jdbc:mysql://rm-2ze08w9zb80nc7582.mysql.rds.aliyuncs.com:3306/bt_bd_etl_metadata";
        HikariDataSource dataSource = JDBCUtils.getDataSource(jdbcUrl, "etl_metadata", "Etl_metadata@2019");
        QueryRunner qr = new QueryRunner(dataSource);
        String sql = "select db_code as code,db_name as dwName from bt_bd_etl_metadata.src_sys_db_info where id=1";
        Info info = qr.query(sql, new BeanHandler<>(Info.class));
        System.out.println(info);
    }


    @Setter
    @Getter
    @ToString
    public static class Info {
        private String code;
        private String dwName;
    }
}
