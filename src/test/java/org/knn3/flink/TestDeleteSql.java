package org.knn3.flink;

import com.knn3.rt.scene.ifcondition.constant.Cons;
import com.knn3.rt.scene.ifcondition.utils.JDBCUtils;
import org.apache.commons.dbutils.QueryRunner;

import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Author apophis
 * @File TestDeleteSql
 * @Time 2022/3/28 13:35
 * @Description 工程描述
 */
public class TestDeleteSql {
    public static void main(String[] args) throws PropertyVetoException, SQLException {
        JDBCUtils.loadDriver(JDBCUtils.POSTGRES);
        String jdbcUrl = "11111";
        String userName = "11111";
        String password = "11111";
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource(jdbcUrl, userName, password));

        List<String> arr = new ArrayList<>();

        // 测试 FINANCE
        // qr.update(Cons.FINANCE_INSERT, UUID.randomUUID(), "1", "1", "1", "1", "1", "1", "1", 1L, true, 1, 1, 1L);
        // qr.update(Cons.FINANCE_INSERT, UUID.randomUUID(), "2", "2", "2", "2", "2", "2", "2", 2L, true, 2, 2, 2L);
        // arr.add("1");
        // arr.add("2");
        // Object[] array = arr.toArray();
        // String delSql = String.format(Cons.FINANCE_DELETE, IntStream.range(0, arr.size()).mapToObj(x -> "?").collect(Collectors.joining(",")));
        // qr.update(delSql, array);


        // 测试 STATUS
        // qr.update(Cons.STATUS_INSERT, UUID.randomUUID(), "1", Cons.DCP_TABLE, "1");
        // qr.update(Cons.STATUS_INSERT, UUID.randomUUID(), "2", Cons.DCP_TABLE, "2");
        arr.add("1");
        arr.add("2");
        Object[] array = arr.toArray();
        String delStatusSql = String.format(Cons.STATUS_DELETE, Cons.DCP_TABLE, IntStream.range(0, arr.size()).mapToObj(x -> "?").collect(Collectors.joining(",")));
        qr.update(delStatusSql, array);
    }
}
