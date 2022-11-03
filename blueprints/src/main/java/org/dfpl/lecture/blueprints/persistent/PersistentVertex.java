package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Direction;
import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class PersistentVertex implements Vertex {
    private Graph g;
    private String id;
    private HashMap<String, Object> properties;

    public PersistentVertex(Graph g, String id) throws SQLException {
        this.g = g;
        this.id = id;
        this.properties = new HashMap<String, Object>();
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Object getProperty(String key) {
        Object value = null;
        try {
            PreparedStatement pst = DBConnection.getInstance().getConnection().prepareStatement("SELECT * FROM Vertex_properties where vertex_id=? and `key`=?");
            pst.setString(1, this.id);
            pst.setString(2, key);
            ResultSet rs = pst.executeQuery();
            rs.first();
            value = rs.getObject("value");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return value;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> s = new HashSet<>();
        try {
            Statement stmt = DBConnection.getInstance().getConnection().createStatement();
            ResultSet rs = stmt.executeQuery("select * from Vertex_properties where vertex_id='" + this.id + "'");
            while (rs.next()) {
                s.add(rs.getString("key"));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return s;
    }

    @Override
    public void setProperty(String key, Object value) {
        try {
            Statement stmt = DBConnection.getInstance().getConnection().createStatement();
            String sql = "INSERT INTO Vertex_properties VALUES('" + this.id + "','"+key+ "','" +value+"')";

            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public Object removeProperty(String key) {
        return null;
    }

    @Override
    public Collection<Edge> getEdges(Direction direction, String... labels) throws IllegalArgumentException {
        return null;
    }

    @Override
    public Collection<Vertex> getVertices(Direction direction, String... labels) throws IllegalArgumentException {
        return null;
    }

    @Override
    public Collection<Vertex> getVertices(Direction direction, String key, Object value, String... labels) throws IllegalArgumentException {
        return null;
    }

    @Override
    public void remove() {

    }


    public static Vertex selectVertex(Graph g, String id) throws SQLException {
        Statement stmt = DBConnection.getInstance().getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM Vertex WHERE vertex_id = " + id);

        if (!rs.next()) {
            return null;
        }

        return new PersistentVertex(g, id);
    }

    public static Vertex createVertex(Graph g, String id) throws SQLException {
        Statement stmt = DBConnection.getInstance().getConnection().createStatement();
        String q = "INSERT INTO Vertex VALUES(" + id  + ");";
        stmt.executeUpdate(q);

        return new PersistentVertex(g, id);
    }

    public static Collection<Vertex> selectVertices(Graph g) throws SQLException {
        Statement stmt = DBConnection.getInstance().getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM Vertex");

        ArrayList<Vertex> vertices = new ArrayList<>();
        while (rs.next()) {
            vertices.add(new PersistentVertex(g, rs.getString("vertex_id")));
        }

        return vertices;
    }
}
