// package org.knn3.flink;
//
// import org.apache.ibatis.io.Resources;
// import org.apache.ibatis.session.SqlSession;
// import org.apache.ibatis.session.SqlSessionFactory;
// import org.apache.ibatis.session.SqlSessionFactoryBuilder;
// import org.junit.Test;
//
// import java.io.IOException;
// import java.io.Reader;
// import java.sql.Connection;
//
// public class MyBatisTest {
// //    @Test
// //    public void testSqlSessionFactory() throws IOException {
// //        //getResource读取配置文件  AsReader按照字符流的方式进行读取
// //        //getResourceAsReader返回Reader，Reader包含XML的文本信息
// //        Reader reader = Resources.getResourceAsReader("mybatis-config.xml");
// //        //利用构造者模式来初始化SqlSessionFactory对象，同时解析mybatis-config.xml配置文件
// //        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
// //        //测试SqlSessionFactory是否初始化成功，并不意味着已经连接数据库
// //        //System.out.println("加载成功");
// //        SqlSession sqlSession = null;
// //        //创建SqlSession对象，SqlSession是JDBC的扩展类，用于与数据库进行交互
// //        try {
// //            sqlSession = sqlSessionFactory.openSession();
// //            //在正常开发时，MyBatis会自动帮我们完成来连接动作，此处是测试使用
// //            Connection connection = sqlSession.getConnection();
// //            System.out.println(connection);
// //            //com.mysql.jdbc.JDBC4Connection@370736d9,连接已创建
// //        } catch (Exception e) {
// //            e.printStackTrace();
// //        } finally {
// //            if (sqlSession != null) {
// //                //如果type="POOLED"，代表使用连接池，close则是将连接回收到连接池中
// //                //如果type="UNPOOLED",代表直连，close则会调用Connection.close()来关闭连接
// //                sqlSession.close();
// //            }
// //        }
// //    }
//     @Test
//  public void testReadConfig() throws IOException {
//         Reader reader = Resources.getResourceAsReader("mybatis-config.xml");
//
//     }
//
// }
