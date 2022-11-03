package org.dfpl.lecture.blueprints.persistent;

import com.tinkerpop.blueprints.revised.Direction;
import com.tinkerpop.blueprints.revised.Edge;
import com.tinkerpop.blueprints.revised.Vertex;

import java.util.Set;

public class PersistentEdge implements Edge {
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        return null;
    }

    @Override
    public String getLabel() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public String getId() {
        return null;
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
