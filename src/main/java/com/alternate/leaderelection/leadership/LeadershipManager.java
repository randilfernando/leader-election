package com.alternate.leaderelection.leadership;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * @author randilfernando
 */
public class LeadershipManager {

    private final String topic;
    private final Node candidate;
    private final Map<String, Node> leaders;

    private final Flux<Node> leaderStream;
    private final FluxSink<Node> leaderSink;
    private final Flux<Status> statusStream;
    private final FluxSink<Status> statusSink;

    private Status status;
    private Node leader;

    public LeadershipManager(String topic, Node candidate, Map<String, Node> leaders) {
        this(topic, candidate, leaders, Status.FOLLOWER);
    }

    public LeadershipManager(String topic, Node candidate, Map<String, Node> leaders, Status status) {
        this.topic = topic;
        this.candidate = candidate;
        this.leaders = leaders;
        this.status = status;
        this.leader = null;

        final DirectProcessor<Node> leaderDirectProcessor = DirectProcessor.create();
        final DirectProcessor<Status> statusDirectProcessor = DirectProcessor.create();

        this.leaderStream = leaderDirectProcessor.onBackpressureBuffer();
        this.leaderSink = leaderDirectProcessor.sink();
        this.statusStream = statusDirectProcessor.onBackpressureBuffer();
        this.statusSink = statusDirectProcessor.sink();
    }

    public String getTopic() {
        return topic;
    }

    public Node getCandidate() {
        return candidate;
    }

    public Flux<Node> getLeaderStream() {
        return this.leaderStream;
    }

    public Flux<Status> getStatusStream() {
        return this.statusStream;
    }

    public Mono<Result> acquireLeadership() {
        return Mono.just(this.candidate)
                .map(candidate -> {
                    if (this.leader == null || !this.leader.equals(this.candidate)) {
                        Node candidateClone = candidate.withVersion(1);
                        boolean status = this.leaders.replace(this.topic, this.leader, candidateClone);
                        return this.handleReplaceResult(status, candidateClone);
                    } else {
                        return Result.SUCCESS;
                    }
                });
    }

    public Mono<Result> retainLeadership() {
        return Mono.just(this.candidate)
                .map(candidate -> {
                    if (this.leader.equals(this.candidate)) {
                        Node leaderClone = this.leader.withVersion(this.leader.getVersion() + 1);
                        boolean status = this.leaders.replace(this.topic, this.leader, leaderClone);
                        return this.handleReplaceResult(status, leaderClone);
                    } else {
                        return Result.FAILED;
                    }
                });
    }

    public void start() {
        this.getStatusStream().subscribe(status -> this.status = status);
        acquireLeadership().subscribe();
    }

    private Result handleReplaceResult(boolean status, Node candidateWrapper) {
        if (status) {
            this.leader = candidateWrapper;
            this.putIntoFluxSink(candidateWrapper);
            return Result.SUCCESS;
        } else {
            Node leaderWrapper = this.leaders.get(this.topic);
            this.putIntoFluxSink(leaderWrapper);
            return Result.FAILED;
        }
    }

    private void putIntoFluxSink(Node leader) {
        if (leader != null) {
            if (this.status == Status.FOLLOWER && leader.equals(this.candidate)) {
                this.statusSink.next(Status.LEADER);
            } else if (this.status == Status.LEADER && !leader.equals(this.candidate)) {
                this.statusSink.next(Status.LEADER);
            }

            this.leaderSink.next(leader);
        }
    }
}
