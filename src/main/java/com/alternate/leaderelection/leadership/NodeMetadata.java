package com.alternate.leaderelection.leadership;

public class NodeMetadata {
    private long version;

    private NodeMetadata() {
    }

    public NodeMetadata(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeMetadata)) return false;

        NodeMetadata nm = (NodeMetadata) obj;

        return (nm.version == this.version);
    }

    @Override
    protected NodeMetadata clone() {
        return new NodeMetadata(this.version);
    }
}
