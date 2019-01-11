package com.alternate.leaderelection.leadership;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Map;

public class LeadershipManager {
    private final String topic;
    private final Node candidate;
    private final Map<String, NodeWrapper> leaders;

    private final Flux<Node> leaderStream;
    private final FluxSink<Node> leaderSink;
    private final Flux<Status> statusStream;
    private final FluxSink<Status> statusSink;

    private Status status;
    private NodeWrapper leaderWrapper;

    public LeadershipManager(String topic, Node candidate, Map<String, NodeWrapper> leaders) {
        this(topic, candidate, leaders, Status.FOLLOWER);
    }

    public LeadershipManager(String topic, Node candidate, Map<String, NodeWrapper> leaders, Status status) {
        this.topic = topic;
        this.candidate = candidate;
        this.leaders = leaders;
        this.status = status;
        this.leaderWrapper = null;

        final DirectProcessor<Node> leaderDirectProcessor = DirectProcessor.create();
        final DirectProcessor<Status> candidateStatusDirectProcessor = DirectProcessor.create();

        this.leaderStream = leaderDirectProcessor.onBackpressureBuffer();
        this.leaderSink = leaderDirectProcessor.sink();
        this.statusStream = candidateStatusDirectProcessor.onBackpressureBuffer();
        this.statusSink = candidateStatusDirectProcessor.sink();
    }

    public String getTopic() {
        return topic;
    }

    public Node getNode() {
        return candidate;
    }

    public Flux<Node> listenForLeaderChanges() {
        return this.leaderStream;
    }

    public Flux<Status> listenForStatusChanges() {
        return this.statusStream;
    }

    public Mono<Result> acquireLeadership() {
        return Mono.just(this.candidate.getId()).map(id -> {
            if (this.leaderWrapper == null || !this.leaderWrapper.getNode().getId().equals(this.candidate.getId())) {
                NodeWrapper candidateWrapper = new NodeWrapper(this.candidate, 1);
                boolean status = this.leaders.replace(this.topic, this.leaderWrapper, candidateWrapper);
                return this.handleReplaceResult(status, candidateWrapper);
            } else {
                return Result.SUCCESS;
            }
        });
    }

    public Mono<Result> retainLeadership() {
        return Mono.just(this.candidate.getId()).map(id -> {
            if (this.leaderWrapper.getNode().getId().equals(this.candidate.getId())) {
                NodeWrapper candidateWrapper = new NodeWrapper(this.candidate, this.leaderWrapper.getVersion() + 1);
                boolean status = this.leaders.replace(this.topic, this.leaderWrapper, candidateWrapper);
                return this.handleReplaceResult(status, candidateWrapper);
            } else {
                return Result.FAILED;
            }
        });
    }

    public void start() {
        this.listenForStatusChanges().subscribe(status -> this.status = status);

        NodeWrapper candidateWrapper = new NodeWrapper(this.candidate, 1);
        NodeWrapper leaderWrapper = this.leaders.putIfAbsent(this.topic, candidateWrapper);
        this.leaderWrapper = leaderWrapper;
        this.putIntoFluxSink(leaderWrapper);
    }

    private Result handleReplaceResult(boolean status, NodeWrapper candidateWrapper) {
        if (status) {
            this.leaderWrapper = candidateWrapper;
            this.putIntoFluxSink(candidateWrapper);
            return Result.SUCCESS;
        } else {
            NodeWrapper leaderWrapper = this.leaders.get(this.topic);
            this.putIntoFluxSink(leaderWrapper);
            return Result.FAILED;
        }
    }

    private void putIntoFluxSink(NodeWrapper leaderWrapper) {
        if (leaderWrapper != null) {
            if (this.leaderWrapper == null || !this.leaderWrapper.getNode().getId().equals(leaderWrapper.getNode().getId())) {
                this.leaderSink.next(leaderWrapper.getNode());
            }

            if (this.status == Status.FOLLOWER && leaderWrapper.getNode().getId().equals(this.candidate.getId())) {
                this.statusSink.next(Status.LEADER);
            } else if (this.status == Status.LEADER && !leaderWrapper.getNode().getId().equals(this.candidate.getId())) {
                this.statusSink.next(Status.LEADER);
            }
        }
    }
}
