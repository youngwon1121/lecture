package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Direction;
import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;
import org.json.JSONArray;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class PersistentEdge implements Edge {
    private Graph g;
    private String id;
    private Vertex in_vertex; // id만 저장?
    private Vertex out_vertex;
    private String label;
    public PersistentEdge(Graph g, String id, Vertex in_vertex, Vertex out_vertex,String label){
        this.id = id;
        this.in_vertex = in_vertex;
        this.out_vertex = out_vertex;
        this.label = label;
    }
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        return null;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public void remove() {

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
            String sql = "SELECT JSON_TYPE(value), value FROM (SELECT JSON_VALUE(edge_property, '$."+key+"') AS value FROM Edge WHERE edge_id = "+this.id+") AS t;";
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
            String sql = "SELECT JSON_KEYS(edge_property) FROM Edge WHERE edge_id = ?;";
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
            String sql = "UPDATE Edge SET edge_property = JSON_SET(edge_property, '$."+key+"',?) WHERE edge_id = ?;";
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
            String sql = "UPDATE Edge SET edge_property = JSON_REMOVE(edge_property, '$."+key+"') WHERE edge_id = ?;";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,this.id);
            pstmt.setString(2,this.id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }
}
