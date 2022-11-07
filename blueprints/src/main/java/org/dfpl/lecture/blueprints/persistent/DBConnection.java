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

        stmt.executeUpdate("CREATE OR REPLACE table Vertex(" +
                "    vertex_id varchar(30)," +
                "    vertex_property JSON DEFAULT json_object()," +
                "    PRIMARY KEY (vertex_id)" +
                ");");
        stmt.executeUpdate("CREATE OR REPLACE table Edge(" +
                "    edge_id varchar(30)," +
                "    in_vertex_id varchar(30) not null," +
                "    out_vertex_id varchar(30) not null," +
                "    edge_label varchar(30)," +
                "    edge_property JSON DEFAULT json_object()," +
                "    PRIMARY KEY (edge_id)," +
                "    FOREIGN KEY (in_vertex_id) REFERENCES Vertex(vertex_id)," +
                "    FOREIGN KEY (out_vertex_id) REFERENCES Vertex(vertex_id)" +
                ");");
    }

    public static DBConnection getInstance() {
        return instance;
    }

    public Connection getConnection() {
        return conn;
    }
}
