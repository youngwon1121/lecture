package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

public class PersistentGraph implements Graph {
    private Connection conn;


    public PersistentGraph(String dbID, String dbPW, String dbName) throws SQLException {
        DBConnection.init(dbID, dbPW, dbName);
        conn = DBConnection.getInstance().getConnection();

        Statement stmt = conn.createStatement();
        stmt.executeUpdate("CREATE OR REPLACE table Vertex(" +
                "    vertex_id varchar(30)," +
                "    PRIMARY KEY (vertex_id)" +
                ");");
        stmt.executeUpdate("CREATE OR REPLACE table Edge(" +
                "    edge_id varchar(30) not null unique," +
                "    in_vertex_id varchar(30)," +
                "    out_vertex_id varchar(30)," +
                "    edge_label varchar(30)," +
                "    PRIMARY KEY (in_vertex_id, out_vertex_id, edge_label)," +
                "    FOREIGN KEY (in_vertex_id) REFERENCES Vertex(vertex_id)," +
                "    FOREIGN KEY (out_vertex_id) REFERENCES Vertex(vertex_id)" +
                ");");

        stmt.executeUpdate("CREATE OR REPLACE table Vertex_properties (" +
                "    vertex_id varchar(30)," +
                "    `key` varchar(30)," +
                "    `value` varchar(30)" +
                ");");
    }

    @Override
    public Vertex addVertex(String id) throws IllegalArgumentException {
        if (id.contains("|")) {
            throw new IllegalArgumentException("id cannot contain '|'");
        }

        Vertex newV = null;
        try {
            Vertex v = PersistentVertex.selectVertex(this, id);
            if(v != null) return v;

            newV = PersistentVertex.createVertex(this, id);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return newV;
    }

    @Override
    public Vertex getVertex(String id) {
        Vertex v = null;
        try {
            v = PersistentVertex.selectVertex(this, id);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return v;
    }

    @Override
    public void removeVertex(Vertex vertex) {

    }

    @Override
    public Collection<Vertex> getVertices() {
        Collection<Vertex> c = null;
        try {
            c = PersistentVertex.selectVertices(this);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return c;
    }

    @Override
    public Collection<Vertex> getVertices(String key, Object value) {
        return null;
    }

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label) throws IllegalArgumentException, NullPointerException {
        return null;
    }

    @Override
    public Edge getEdge(Vertex outVertex, Vertex inVertex, String label) {
        return null;
    }

    @Override
    public Edge getEdge(String id) {
        return null;
    }

    @Override
    public void removeEdge(Edge edge) {

    }

    @Override
    public Collection<Edge> getEdges() {
        return null;
    }

    @Override
    public Collection<Edge> getEdges(String key, Object value) {
        return null;
    }

    @Override
    public void shutdown() {

    }
}
