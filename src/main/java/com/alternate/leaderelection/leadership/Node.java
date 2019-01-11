package com.alternate.leaderelection.leadership;

public class Node {
    private String id;

    private Node() {
    }

    public Node(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Node)) return false;
        Node n = (Node) o;
        return (n.id.equals(this.id));
    }
}
