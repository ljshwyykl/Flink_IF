package org.knn3.flink.factory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSourceFactory;

/*
 * C3P0与MyBatis兼容使用的数据源工厂类
 * */
public class C3P0DataSourceFactory extends UnpooledDataSourceFactory {
    public C3P0DataSourceFactory(){
        //数据源由C3P0负责创建
        this.dataSource = new ComboPooledDataSource();
    }
}