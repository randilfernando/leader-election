package com.alternate.leaderelection.leadership;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LeadershipManager {
    private final String topic;
    private final Node candidate;
    private final Map<String, NodeWrapper> leaders;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Flux<Node> leaderStream;
    private final FluxSink<Node> leaderSink;
    private final Flux<Status> statusStream;
    private final FluxSink<Status> statusSink;

    private Status status;
    private NodeWrapper leaderWrapper;

    public LeadershipManager(String topic, Node candidate, Map<String, NodeWrapper> leaders) {
        this(topic, candidate, leaders, Status.CANDIDATE, Executors.newSingleThreadScheduledExecutor());
    }

    public LeadershipManager(String topic, Node candidate, Map<String, NodeWrapper> leaders, Status status) {
        this(topic, candidate, leaders, status, Executors.newSingleThreadScheduledExecutor());
    }

    public LeadershipManager(String topic, Node candidate, Map<String, NodeWrapper> leaders, ScheduledExecutorService scheduledExecutorService) {
        this(topic, candidate, leaders, Status.CANDIDATE, scheduledExecutorService);
    }

    public LeadershipManager(String topic, Node candidate, Map<String, NodeWrapper> leaders, Status status, ScheduledExecutorService scheduledExecutorService) {
        this.topic = topic;
        this.candidate = candidate;
        this.leaders = leaders;
        this.scheduledExecutorService = scheduledExecutorService;

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
                NodeWrapper candidateWrapper = new NodeWrapper(this.candidate, new NodeMetadata(1));

                boolean status = this.leaders.replace(this.topic, this.leaderWrapper, candidateWrapper);

                if (status) {
                    this.setLeaderWrapper(candidateWrapper);
                    return Result.SUCCESS;
                } else {
                    this.setLeaderWrapper(this.leaders.get(this.topic));
                    return Result.FAILED;
                }
            } else {
                return Result.SUCCESS;
            }
        });
    }

    public void start(int heartbeatInterval, int leaderCheckInterval) {
        this.listenForStatusChanges().subscribe(status -> this.status = status);

        NodeWrapper leaderWrapper = this.leaders.putIfAbsent(this.topic, new NodeWrapper(this.candidate, new NodeMetadata(1)));

        this.setLeaderWrapper(leaderWrapper);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (this.status == Status.LEADER) {
                this.sendHeartBeat();
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (this.status == Status.FOLLOWER) {
                this.checkLeader();
            }
        }, leaderCheckInterval, leaderCheckInterval, TimeUnit.SECONDS);
    }

    private void checkLeader() {
        NodeWrapper leaderWrapper = this.leaders.get(this.topic);

        this.setLeaderWrapper(leaderWrapper);
    }

    private void sendHeartBeat() {
        NodeWrapper candidateWrapper = this.leaderWrapper.clone();
        candidateWrapper.getNodeMetadata().setVersion(candidateWrapper.getNodeMetadata().getVersion() + 1);

        boolean status = this.leaders.replace(this.topic, this.leaderWrapper, candidateWrapper);

        if (status) {
            this.setLeaderWrapper(candidateWrapper);
        } else {
            NodeWrapper leaderWrapper = this.leaders.get(this.topic);
            this.setLeaderWrapper(leaderWrapper);
        }
    }

    private void setLeaderWrapper(NodeWrapper leaderWrapper) {
        if (leaderWrapper != null) {
            NodeWrapper oldLeaderWrapper = this.leaderWrapper;
            this.leaderWrapper = leaderWrapper;

            if (oldLeaderWrapper == null || !oldLeaderWrapper.getNode().equals(leaderWrapper.getNode())) {
                this.leaderSink.next(leaderWrapper.getNode());
            }

            if ((this.status == Status.FOLLOWER || this.status == Status.CANDIDATE) &&
                    leaderWrapper.getNode().equals(this.candidate)) {
                this.statusSink.next(Status.LEADER);
            } else if ((this.status == Status.LEADER || this.status == Status.CANDIDATE) &&
                    !leaderWrapper.getNode().equals(this.candidate)) {
                this.statusSink.next(Status.FOLLOWER);
            } else if (oldLeaderWrapper != null &&
                    this.status == Status.FOLLOWER &&
                    leaderWrapper.getNodeMetadata().getVersion() <= oldLeaderWrapper.getNodeMetadata().getVersion()) {
                this.statusSink.next(Status.CANDIDATE);
            }
        }
    }
}
