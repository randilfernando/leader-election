package com.alternate.leaderelection.leadership;

public class NodeWrapper {
    private Node node;
    private NodeMetadata nodeMetadata;

    private NodeWrapper() {
    }

    public NodeWrapper(Node node, NodeMetadata nodeMetadata) {
        this.node = node;
        this.nodeMetadata = nodeMetadata;
    }

    public Node getNode() {
        return node;
    }

    public NodeMetadata getNodeMetadata() {
        return nodeMetadata;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeWrapper)) return false;

        NodeWrapper nw = (NodeWrapper) obj;

        return (nw.node.equals(this.node) &&
                nw.nodeMetadata.equals(this.nodeMetadata));
    }

    @Override
    protected NodeWrapper clone() {
        return new NodeWrapper(this.node.clone(), this.nodeMetadata.clone());
    }
}
