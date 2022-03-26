package com.knn3.rt.scene.ifcondition.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.beans.PropertyVetoException;

/**
 * @author
 * @ClassName JDBCUtils
 * @date 2020/5/23
 * @desc 无
 */
public class JDBCUtils {
    public final static String MYSQL = "com.mysql.cj.jdbc.Driver";
    public final static String POSTGRES = "org.postgresql.Driver";
    //创建连接池,只创建一次,所以使用static final
    private static HikariDataSource ds = null;
    private static String DRIVER = null;

    public static void loadDriver(String driverClassName) {
        JDBCUtils.DRIVER = driverClassName;
    }

    //获取连接池
    public static HikariDataSource getDataSource(String jdbcUrl, String userName, String passWord) throws PropertyVetoException {
        if (JDBCUtils.ds == null) {
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(JDBCUtils.DRIVER);
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
}
