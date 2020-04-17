package com.alternate.leaderelection.leadership;

/**
 * @author randilfernando
 */
public class Node {

    private String id;
    private long version = 0;

    // For jackson
    private Node() {
    }

    public Node(String id) {
        this.id = id;
        this.version = 0;
    }

    public Node(String id, long version) {
        this.id = id;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Node withVersion(long version) {
        Node clone = clone();
        clone.setVersion(version);
        return clone;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Node)) return false;
        Node n = (Node) o;
        return (n.id.equals(this.id));
    }

    @Override
    protected Node clone() {
        return new Node(id, version);
    }
}
