package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PersistentGraph implements Graph {
    private Connection conn;


    public PersistentGraph(String dbID, String dbPW, String dbName) throws SQLException {
        DBConnection.init(dbID, dbPW, dbName);
        conn = DBConnection.getInstance().getConnection();
    }

    @Override
    public Vertex addVertex(String id) throws IllegalArgumentException {


        if (id.contains("|")) {
            throw new IllegalArgumentException("id cannot contain '|'");
        }

        Vertex v = null;

        v = this.getVertex(id);
        if (v != null) return v;

        PreparedStatement pst = null;
        String q;
        try {
            q = "INSERT INTO Vertex(vertex_id) VALUES(?)";
            pst = conn.prepareStatement(q);
            pst.setString(1, id);
            pst.executeUpdate();
            v = new PersistentVertex(this, id);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return v;
    }

    @Override
    public Vertex getVertex(String id) {
        Vertex v = null;

        try {
            String q = "SELECT * FROM Vertex WHERE vertex_id = ?";
            PreparedStatement pst = null;
            pst = conn.prepareStatement(q);
            pst.setString(1,id);
            ResultSet rs = pst.executeQuery();

            if (!rs.next()) {return v;}

            v = new PersistentVertex(this, rs.getString("vertex_id"));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return v;
    }

    @Override
    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }

    @Override
    public Collection<Vertex> getVertices() {
        List<Vertex> vertices = new ArrayList<Vertex>();

        try {
            String q = "SELECT * FROM Vertex";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(q);

            while (rs.next()) {
                vertices.add(new PersistentVertex(this, rs.getString("vertex_id")));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return vertices;
    }

    @Override
    public Collection<Vertex> getVertices(String key, Object value) {
        List<Vertex> vertices = new ArrayList<Vertex>();

        try {
            String q = "SELECT * FROM Vertex WHERE JSON_EXTRACT(vertex_property, '$." + key + "') = ?";
            PreparedStatement pst = conn.prepareStatement(q);
            pst.setObject(1, value);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                vertices.add(new PersistentVertex(this, rs.getString("vertex_id")));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return vertices;
    }

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label) throws IllegalArgumentException, NullPointerException {
        if (label.contains("|")) {
            throw new IllegalArgumentException("label cannot contain '|'");
        }

        if (outVertex == null) {
            throw new NullPointerException("outVertex cannot be null");
        }

        if (inVertex == null) {
            throw new NullPointerException("inVertex cannot be null");
        }

        String edge_id = this.generateEdgeId(outVertex, inVertex, label);
        Edge e = null;

        e = this.getEdge(outVertex, inVertex, label);
        if (e != null) return e;

        PreparedStatement pst = null;
        String q;
        try {
            q = "INSERT INTO Edge(edge_id, in_vertex_id, out_vertex_id, edge_label) VALUES(?,?,?,?)";
            pst = conn.prepareStatement(q);
            pst.setString(1, edge_id);
            pst.setString(2, inVertex.getId());
            pst.setString(3, outVertex.getId());
            pst.setString(4, label);
            pst.executeUpdate();
            e = new PersistentEdge(this, edge_id, inVertex, outVertex, label);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return e;
    }

    @Override
    public Edge getEdge(Vertex outVertex, Vertex inVertex, String label) {
        Edge v = null;
        String edge_id = this.generateEdgeId(outVertex, inVertex, label);

        try {
            String q = "SELECT * FROM Edge WHERE edge_id = ?";
            PreparedStatement pst = null;
            pst = conn.prepareStatement(q);
            pst.setString(1, edge_id);
            ResultSet rs = pst.executeQuery();

            if (!rs.next()) {return v;}

            v = new PersistentEdge(this, edge_id, inVertex, outVertex, label);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return v;
    }

    @Override
    public Edge getEdge(String id) {
        Edge v = null;
        try {
            String q = "SELECT * FROM Edge WHERE edge_id = ?";
            PreparedStatement pst = null;
            pst = conn.prepareStatement(q);
            pst.setString(1, id);
            ResultSet rs = pst.executeQuery();

            if (!rs.next()) {return v;}

            Vertex inv, outv;
            inv = this.getVertex(rs.getString("in_vertex_id"));
            outv = this.getVertex(rs.getString("out_vertex_id"));
            v = new PersistentEdge(this, id, inv, outv, rs.getString("edge_label"));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return v;
    }

    @Override
    public void removeEdge(Edge edge) {
        edge.remove();
    }

    @Override
    public Collection<Edge> getEdges() {
        List<Edge> edges = new ArrayList<Edge>();

        try {
            String q = "SELECT * FROM Edge";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(q);
            Vertex inv, outv;
            String edge_id;
            while (rs.next()) {
                inv = this.getVertex(rs.getString("in_vertex_id"));
                outv = this.getVertex(rs.getString("out_vertex_id"));
                edge_id = this.generateEdgeId(outv,inv, rs.getString("edge_label"));
                edges.add(new PersistentEdge(this, edge_id, inv, outv, rs.getString("edge_label")));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return edges;
    }

    @Override
    public Collection<Edge> getEdges(String key, Object value) {
        List<Edge> edges = new ArrayList<Edge>();

        try {
            String q = "SELECT * FROM Edge WHERE JSON_EXTRACT(Edge_property, '$." + key + "') = ?";
            PreparedStatement pst = conn.prepareStatement(q);
            pst.setObject(1, value);
            ResultSet rs = pst.executeQuery();
            Vertex inv, outv;
            String edge_id;
            while (rs.next()) {
                inv = this.getVertex(rs.getString("in_vertex_id"));
                outv = this.getVertex(rs.getString("out_vertex_id"));
                edge_id = this.generateEdgeId(outv,inv, rs.getString("edge_label"));
                edges.add(new PersistentEdge(this, edge_id, inv, outv, rs.getString("edge_label")));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return edges;
    }

    @Override
    public void shutdown() {
        try {
            DBConnection.getInstance().getConnection().close();
        }catch (SQLException e){e.printStackTrace();}
    }

    private String generateEdgeId(Vertex outVertex, Vertex inVertex, String label) {
        return outVertex.getId() + "|" + label + "|" + inVertex.getId();
    }
}
