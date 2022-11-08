package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Direction;
import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;

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
        try {
            String in_vertex_sql = "SELECT edge_id, edge_label FROM Edge WHERE in_vertex_id = ? ";
            String out_vertex_sql = "SELECT edge_id, edge_label FROM Edge WHERE out_vertex_id = ? ";

            String q ="";
            if (labels.length > 0) {
                q += "and edge_label in ('";
                q += String.join("','", labels);
                q += "')";

                in_vertex_sql += q;
                out_vertex_sql += q;
            }

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
                // 성능을 위해 후에 수정 필요
                while(rs.next()){
                    String edge_id = rs.getString("edge_id");
                    String edge_label = rs.getString("edge_label");
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
        try {
            String in_vertex_sql = "SELECT edge_label, in_vertex_id AS vertex_id FROM Edge WHERE out_vertex_id = ?;";
            String out_vertex_sql = "SELECT edge_label, out_vertex_id AS vertex_id FROM Edge WHERE in_vertex_id = ?;";
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
                    // 성능을 위해 후에 수정 필요
                    while(rs.next()){
                        String edge_label = rs.getString("edge_label");
                        String vertex_id = rs.getString("vertex_id");
                        for (var l:labels ) {
                            if(l == edge_label){
                                ret.add(new PersistentVertex(g, vertex_id));
                                break;
                            }
                        }
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
        for (Vertex v : this.getVertices(direction, labels)) {
            for (var vv: v.getVertices(direction, labels)) {
                ret.add(vv);
            }
        }
        return ret;
    }

    @Override
    public Collection<Vertex> getVertices(Direction direction, String key, Object value, String... labels) throws IllegalArgumentException {
        ArrayList<Vertex> ret = new ArrayList<>();

        Connection conn = DBConnection.getInstance().getConnection();
        Statement stmt = null;
        try {
            String in_vertex_sql = "SELECT edge_label, in_vertex_id AS res_vertex_id, vertex_property FROM Edge NATURAL JOIN Vertex WHERE out_vertex_id = vertex_id;";
            String out_vertex_sql = "SELECT edge_label, out_vertex_id AS res_vertex_id, vertex_property FROM Edge NATURAL JOIN Vertex WHERE in_vertex_id = vertex_id;";
            stmt = conn.createStatement();
            ResultSet rs = null;
            switch (direction){
                case IN: case OUT:
                    if(direction == Direction.IN) {
                        rs = stmt.executeQuery(in_vertex_sql);
                    }
                    if(direction == Direction.OUT) {
                        rs = stmt.executeQuery(out_vertex_sql);
                    }
                    // 성능을 위해 후에 수정 필요
                    while(rs.next()){
                        String edge_label = rs.getString("edge_label");
                        String vertex_id = rs.getString("res_vertex_id");
                        JSONObject property = new JSONObject(rs.getString("vertex_property"));

                        for (var l:labels ) {
                            if(l == edge_label){
                                if(property.get(key).equals(value.toString())) {
                                    ret.add(new PersistentVertex(g, vertex_id));
                                    break;
                                }
                            }
                        }
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
        System.out.println(this.id +" : "+ vertex.id);
        return this.id.equals(vertex.id);
    }
}
