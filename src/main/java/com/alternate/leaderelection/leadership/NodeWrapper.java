package com.alternate.leaderelection.leadership;

public class NodeWrapper {
    private Node node;
    private long version;

    private NodeWrapper() {
    }

    public NodeWrapper(Node node, long version) {
        this.node = node;
        this.version = version;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeWrapper)) return false;

        NodeWrapper nw = (NodeWrapper) obj;

        return (nw.node.equals(this.node) &&
                nw.version == this.version);
    }
}
