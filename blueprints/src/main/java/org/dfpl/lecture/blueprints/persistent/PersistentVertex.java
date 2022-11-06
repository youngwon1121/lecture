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
            String sql = "SELECT JSON_VALUE(vertex_property, '$."+key+"') FROM Vertex WHERE vertex_id = "+this.id+";";
            ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
                return rs.getObject(1);
            }
        }
        catch (SQLException e){ }
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
        catch (SQLException e){ }
        return keySet;
    }

    @Override
    public void setProperty(String key, Object value) {
        // unique 유지
        Connection conn = DBConnection.getInstance().getConnection();
        try {
            String sql = "UPDATE Vertex SET vertex_property = JSON_SET(vertex_property, '$."+key+"',?) WHERE vertex_id = ?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
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
            pstmt.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return ret;
    }

    @Override
    public Collection<Edge> getEdges(Direction direction, String... labels) throws IllegalArgumentException {
        ArrayList<Edge> ret = new ArrayList<>();

        Connection conn = DBConnection.getInstance().getConnection();
        PreparedStatement pstmt = null;
        try {
            String in_vertex_sql = "SELECT edge_id, edge_label AS vertex_id FROM Edge WHERE out_vertex_id = ?;";
            String out_vertex_sql = "SELECT edge_id, edge_label AS vertex_id FROM Edge WHERE in_vertex_id = ?;";
            switch (direction){
                case IN: OUT:
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
                    for (var l:labels ) {
                        if(l == edge_label){
                            Edge e = g.getEdge(edge_id); // query를 한번 더 하는 것이므로 비효율적임
                            // Edge e = new PersistentEdge(edge_id,.....)
                            ret.add(e);
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
        return null;
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
        // 이미 Edge들은 다 지워졌다는 가정하에 코드 실행
        try {
            String sql = "DELETE FROM Vertex WHERE vertex_id = ?;";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, this.id);
        } catch (SQLException e) { e.printStackTrace();}
    }
}
