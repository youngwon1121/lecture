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
    }

    @Override
    public Vertex addVertex(String id) throws IllegalArgumentException {

        return null;
    }

    @Override
    public Vertex getVertex(String id) {
        return null;
    }

    @Override
    public void removeVertex(Vertex vertex) {

    }

    @Override
    public Collection<Vertex> getVertices() {

        return null;
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
