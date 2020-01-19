package sqlpackage;

import java.sql.*;

public class MySQLDemo {
    // MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:4000/test?useSSL=false&useServerPrepStmts=true&useConfigs=maxPerformance&sessionVariables=tidb_batch_commit=1";

    // MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
    //static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    //static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB?useSSL=false&serverTimezone=UTC";


    // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "root";
    static final String PASS = "";

    public static PreparedStatement doPrePare(Connection conn, String sql) throws Exception {
        System.out.printf(">>> prepare statement sql=%s\n", sql);
        PreparedStatement ps = conn.prepareStatement(sql);
        return ps;
    }

    public static void doExecute(PreparedStatement psStmt) throws Exception {
        System.out.println(">>> do execute statement");
        ResultSet rs = psStmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        System.out.println(">>> do print results");
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
        rs.close();
    }

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("connecting to database");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // prepare
            ps = doPrePare(conn, "select c1, c2 from t1 where c1 = ?");
            ps.setInt(1, 1);

            // execute
            doExecute(ps);

            // 完成后关闭
            ps.close();
            conn.close();
        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                if (ps != null) ps.close();
            } catch (SQLException se2) {
            }// 什么都不做
            try {
                if (conn != null) conn.close();
                System.out.println("Finished!");
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
