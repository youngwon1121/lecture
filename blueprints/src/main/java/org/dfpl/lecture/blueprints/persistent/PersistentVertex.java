package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Direction;
import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;
import org.json.JSONArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class PersistentVertex implements Vertex {
    private Graph g;
    private String id;

    public PersistentVertex(Graph g, String id) {
        this.g = g;
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Object getProperty(String key) {
        Connection conn = DBConnection.getInstance().getConnection();
        try {
            Statement stmt = conn.createStatement();
            String sql = "SELECT JSON_TYPE(value), value FROM (SELECT JSON_VALUE(vertex_property, '$."+key+"') AS value FROM Vertex WHERE vertex_id = "+this.id+") AS t;";
            ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
                if(rs.getObject(2) == null) return null;
                if(rs.getObject(1) == null) return rs.getObject(2);
                switch (rs.getString(1)){
                    case "ARRAY" : return rs.getArray(2);
                    case "BOOLEAN" : return rs.getBoolean(2);
                    case "DOUBLE" : return rs.getDouble(2);
                    case "INTEGER" : return rs.getInt(2);
                    case "STRING": return rs.getString(2);
                    default: return rs.getObject(2);
                }
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

        @Override
    public Set<String> getPropertyKeys() {
        HashSet<String> keySet = new HashSet<>();

        Connection conn = DBConnection.getInstance().getConnection();
        try {
            String sql = "SELECT JSON_KEYS(vertex_property) FROM Vertex WHERE vertex_id = ?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, this.id);
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()){
                JSONArray tmp = new JSONArray(rs.getString(1));
                for(int j = 0;j<tmp.length();j++) {
                    keySet.add(tmp.getString(j));
                }
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return keySet;
    }

    @Override
    public void setProperty(String key, Object value) {
        // unique 유지
        Connection conn = DBConnection.getInstance().getConnection();
        try {
            String sql = "UPDATE Vertex SET vertex_property = JSON_SET(vertex_property, '$."+key+"',?) WHERE vertex_id = ?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            if(value.getClass().getName() == "java.lang.Boolean"){
                pstmt.setString(1, Boolean.parseBoolean(value.toString()) == true?"true":"false");
            }
            else
                pstmt.setObject (1,value); // value
            pstmt.setString(2,this.id); // id
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object removeProperty(String key) {
        Object ret = getProperty(key);

        Connection conn = DBConnection.getInstance().getConnection();
        PreparedStatement pstmt;
        try {
            String sql = "UPDATE Vertex SET vertex_property = JSON_REMOVE(vertex_property, '$."+key+"') WHERE vertex_id = ?;";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,this.id);
            pstmt.setString(2,this.id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public Collection<Edge> getEdges(Direction direction, String... labels) throws IllegalArgumentException {
        ArrayList<Edge> ret = new ArrayList<>();

        Connection conn = DBConnection.getInstance().getConnection();
        PreparedStatement pstmt = null;

        // sql String
        String in_vertex_sql = "SELECT edge_id FROM Edge WHERE in_vertex_id = ? ";
        String out_vertex_sql = "SELECT edge_id FROM Edge WHERE out_vertex_id = ? ";
        String q ="";
        if (labels.length > 0) {
            q += "AND edge_label IN ('";
            q += String.join("','", labels);
            q += "');";

            in_vertex_sql += q;
            out_vertex_sql += q;
        }
        try {
            switch (direction){
                case IN: case OUT:
                if(direction == Direction.IN) {
                    pstmt = conn.prepareStatement(in_vertex_sql);
                    pstmt.setString(1, this.id);
                }
                if(direction == Direction.OUT) {
                    pstmt = conn.prepareStatement(out_vertex_sql);
                    pstmt.setString(1, this.id);
                }
                ResultSet rs = pstmt.executeQuery();

                while(rs.next()){
                    String edge_id = rs.getString("edge_id");
                    Edge e = g.getEdge(edge_id);
                    ret.add(e);
                }
                break;
                case BOTH :
                    throw new IllegalArgumentException("Direction.BOTH is not allowed");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public Collection<Vertex> getVertices(Direction direction, String... labels) throws IllegalArgumentException {
        ArrayList<Vertex> ret = new ArrayList<>();

        Connection conn = DBConnection.getInstance().getConnection();
        PreparedStatement pstmt = null;

        // sql String
        String in_vertex_sql = "SELECT out_vertex_id AS vertex_id " +
                "FROM Edge " +
                "WHERE in_vertex_id = ? ";
        String out_vertex_sql = "SELECT in_vertex_id AS vertex_id " +
                "FROM Edge " +
                "WHERE out_vertex_id = ? ";
        String q ="";
        if (labels.length > 0) {
            q += "AND edge_label IN ('";
            q += String.join("','", labels);
            q += "');";

            in_vertex_sql += q;
            out_vertex_sql += q;
        }
        try {
            switch (direction){
                case IN: case OUT:
                    if(direction == Direction.IN) {
                        pstmt = conn.prepareStatement(in_vertex_sql);
                        pstmt.setString(1, this.id);
                    }
                    if(direction == Direction.OUT) {
                        pstmt = conn.prepareStatement(out_vertex_sql);
                        pstmt.setString(1, this.id);
                    }
                    ResultSet rs = pstmt.executeQuery();

                    while(rs.next()){
                        ret.add(new PersistentVertex(this.g, rs.getString("vertex_id")));
                    }
                    break;
                case BOTH :
                    throw new IllegalArgumentException("Direction.BOTH is not allowed");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public Collection<Vertex> getTwoHopVertices(Direction direction, String... labels) throws IllegalArgumentException {
        ArrayList<Vertex> ret = new ArrayList<>();

        Connection conn = DBConnection.getInstance().getConnection();
        PreparedStatement pstmt = null;

        // sql String
        String in_vertex_sql =
                "SELECT e2.out_vertex_id " +
                "FROM Edge AS e1 JOIN Edge AS e2 " +
                "WHERE e1.in_vertex_id = ? AND e1.out_vertex_id = e2.in_vertex_id;";
        String out_vertex_sql =
                "SELECT e2.in_vertex_id " +
                "FROM Edge AS e1 JOIN Edge AS e2 " +
                "WHERE e1.out_vertex_id = ? AND e1.in_vertex_id = e2.out_vertex_id;";
        String q ="";
        if (labels.length > 0) {
            q += "AND edge_label IN ('";
            q += String.join("','", labels);
            q += "');";

            in_vertex_sql += q;
            out_vertex_sql += q;
        }
        try {
            switch (direction) {
                case IN :
                    pstmt = conn.prepareStatement(in_vertex_sql);
                    break;
                case OUT :
                    pstmt = conn.prepareStatement(out_vertex_sql);
                    break;
                case BOTH:
                    throw new IllegalArgumentException("Direction.BOTH is not allowed");
            }
            pstmt.setString(1, this.id);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                ret.add(new PersistentVertex(this.g, rs.getString(1)));
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }

        return ret;
    }

    @Override
    public Collection<Vertex> getVertices(Direction direction, String key, Object value, String... labels) throws IllegalArgumentException {
        ArrayList<Vertex> ret = new ArrayList<>();

        Connection conn = DBConnection.getInstance().getConnection();
        PreparedStatement pstmt = null;

        // sql String
        String in_vertex_sql =
                "SELECT in_vertex_id AS res_vertex_id " +
                "FROM Edge NATURAL JOIN Vertex " +
                "WHERE out_vertex_id = vertex_id AND JSON_CONTAINS(edge_property,?,'$."+key+"') = 1 ";
        String out_vertex_sql =
                "SELECT out_vertex_id AS res_vertex_id " +
                "FROM Edge NATURAL JOIN Vertex " +
                "WHERE in_vertex_id = vertex_id AND JSON_CONTAINS(edge_property,?,'$."+key+"') = 1 ";
        String q ="";
        if (labels.length > 0) {
            q += "AND edge_label IN ('";
            q += String.join("','", labels);
            q += "');";

            in_vertex_sql += q;
            out_vertex_sql += q;
        }

        try {
            switch (direction){
                case IN: case OUT:
                    if(direction == Direction.IN) {
                        pstmt = conn.prepareStatement(in_vertex_sql);

                    }
                    if(direction == Direction.OUT) {
                        pstmt = conn.prepareStatement(out_vertex_sql);
                    }
                    pstmt.setObject(1,value);

                    ResultSet rs = null;
                    while(rs.next()){
                        ret.add(new PersistentVertex(this.g, rs.getString("res_vertex_id")));
                    }
                    break;
                case BOTH :
                    throw new IllegalArgumentException("Direction.BOTH is not allowed");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public void remove() {
        Connection conn = DBConnection.getInstance().getConnection();
        String sql;
        PreparedStatement pstmt;
        try {
            // Edge 삭제
            sql = "DELETE FROM Edge WHERE out_vertex_id = ? OR in_vertex_id = ?;";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,this.id);
            pstmt.setString(2,this.id);
            pstmt.executeUpdate();

            // Vertex 삭제
            sql = "DELETE FROM Vertex WHERE vertex_id = ?;";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, this.id);
            pstmt.executeUpdate();
        } catch (SQLException e) { e.printStackTrace();}
    }

    @Override
    public boolean equals(Object object) {
        PersistentVertex vertex = (PersistentVertex) object;
        return this.id.equals(vertex.id);
    }
}
