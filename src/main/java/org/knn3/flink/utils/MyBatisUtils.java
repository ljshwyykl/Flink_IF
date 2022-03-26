package org.knn3.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;

public class MyBatisUtils {
    //利用static（静态）属于类不属于对象，且全局唯一，static属性本身就属于全局唯一
    private static SqlSessionFactory sqlSessionFactory = null;

    //利用静态块在初始化时实例化sqlSessionFactory
    static {
        Reader reader = null;
        ParameterTool parameter;
        try {
            reader = Resources.getResourceAsReader("mybatis-config.xml");

            // final String fileName = "application.properties";
            // System.out.println("main");
            // InputStream inputStream = MyBatisUtils.class.getClassLoader().getResourceAsStream(fileName);
            // parameter = ParameterTool.fromPropertiesFile(inputStream);
            // System.out.println("app_env:" + parameter.get("app_env"));
        } catch (IOException e) {
            e.printStackTrace();
            //初始化遇到错误时，将异常抛给调用者
            throw new ExceptionInInitializerError(e);
        }
        MyBatisUtils.sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader, "dev");
    }

    //定义返回SqlSession对象的方法
    public static SqlSession openSession() {

        // 默认SqlSession对自动提交事务数据（commit）
        //设置false代表关闭自动提交，改为手动提交事务
        // return sqlSessionFactory.openSession(false);

        return MyBatisUtils.sqlSessionFactory.openSession(false);
    }

    //释放SqlSession对象
    public static void closeSession(SqlSession session) {
        if (session != null) session.close();
    }
}