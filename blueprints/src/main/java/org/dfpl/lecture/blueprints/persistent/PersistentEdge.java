package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Direction;
import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Graph;
import com.tinkerpop.blueprints.revised.Vertex;

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
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {
    }

    @Override
    public Object removeProperty(String key) {
        return null;
    }
}
