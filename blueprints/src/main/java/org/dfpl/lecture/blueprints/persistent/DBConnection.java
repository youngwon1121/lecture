package org.dfpl.lecture.blueprints.persistent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnection {
    private static DBConnection instance = new DBConnection();
    private static Connection conn;
    private DBConnection() {
    }

    public static void init(String dbID, String dbPW, String dbName) throws SQLException {
        conn = DriverManager.getConnection("jdbc:mariadb://localhost:3307", dbID, dbPW);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("CREATE OR REPLACE DATABASE " + dbName);
        stmt.executeUpdate("USE " + dbName);
    }

    public static DBConnection getInstance() {
        return instance;
    }

    public Connection getConnection() {
        return conn;
    }
}
