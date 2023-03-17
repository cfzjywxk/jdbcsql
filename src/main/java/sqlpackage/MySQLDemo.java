package sqlpackage;

import java.sql.*;

public class MySQLDemo {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    // static final String DB_URL = "jdbc:mysql://localhost:4000/test?useSSL=false&useServerPrepStmts=true&useConfigs=maxPerformance&sessionVariables=tidb_batch_commit=1";
    static final String DB_URL = "jdbc:mysql://localhost:4000/test?useSSL=false&useServerPrepStmts=true&useConfigs=maxPerformance&useCursorFetch=true&sessionVariables=tidb_txn_mode='pessimistic',tx_isolation='READ-COMMITTED',tidb_rc_read_check_ts='on', tidb_max_chunk_size=1";

    //static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    //static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB?useSSL=false&serverTimezone=UTC";


    static final String USER = "root";
    static final String PASS = "";

    public static PreparedStatement doPrePare(Connection conn, String sql) throws Exception {
        System.out.printf(">>> prepare statement sql=%s\n", sql);
        PreparedStatement ps = conn.prepareStatement(sql);
	ps.setFetchSize(1);
        return ps;
    }

    public static void doExecute(PreparedStatement psStmt) throws Exception {
        System.out.println(">>> do execute statement");
        ResultSet rs = psStmt.executeQuery();
	rs.setFetchSize(1);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
	int loop = 0;
        System.out.println(">>> do print results");
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.printf(rsmd.getColumnName(i) + ":" + columnValue);
            }
	    System.out.println();
            System.out.printf("sleep 5s before next loop round, loop=%d\n", loop);
	    Thread.currentThread().sleep(1000);
	    loop++;
        }
        rs.close();
    }

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement psCop = null;
        PreparedStatement psBatchGet = null;
        try {
            Class.forName(JDBC_DRIVER);

            System.out.println("connecting to database");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

	    // Init.
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate("set tidb_txn_mode='pessimistic'");
	    stmt.executeUpdate("set tx_isolation='READ-COMMITTED'");
	    stmt.executeUpdate("set tidb_rc_read_check_ts='on'");
	    stmt.executeUpdate("set tidb_max_chunk_size=1");
	    stmt.close();

            // prepare
            // ps = doPrePare(conn, "select c1, c2 from t1 where c1 = ?");
            psCop = doPrePare(conn, "select * from t");
            psBatchGet = doPrePare(conn, "select * from t where a in(1, 2, 3, 1007, 1008, 1009, 104, 105, 106)");
            // ps.setInt(1, 1);

            // execute
	    conn.setAutoCommit(false);
	    Statement stmt2 = conn.createStatement();
	    stmt2.executeUpdate("update t1 set c2 = c2 + 1 where c1 = 1");
	    stmt2.close();
            doExecute(psCop);
            doExecute(psBatchGet);
	    conn.rollback();

            psCop.close();
	    psBatchGet.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (psCop != null) psCop.close();
                if (psBatchGet != null) psBatchGet.close();
            } catch (SQLException se2) {
                try {
                    if (conn != null) conn.close();
                    System.out.println("Finished!");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
	    }
        }
    }
}
