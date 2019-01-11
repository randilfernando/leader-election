package com.alternate.leaderelection.example;

import com.alternate.leaderelection.dynamodb.DynamoDBMap;
import com.alternate.leaderelection.leadership.LeadershipManager;
import com.alternate.leaderelection.leadership.Node;
import com.alternate.leaderelection.leadership.NodeWrapper;

import java.util.Map;

public class LeadershipManagerExample {

    public static void main(String[] args) throws InterruptedException {
        Node candidate = new Node("node-1");
        Map<String, NodeWrapper> leaders = new DynamoDBMap<>(String.class, NodeWrapper.class, "leaders");

        LeadershipManager leadershipManager = new LeadershipManager("group-1", candidate, leaders);

        leadershipManager.listenForLeaderChanges().subscribe(leader -> System.out.printf("Topic leader changed: %s\n", leader.getId()));
        leadershipManager.listenForStatusChanges().subscribe(status -> System.out.printf("Candidate status changed: %s\n", status.toString()));

        leadershipManager.start();

        leadershipManager.acquireLeadership().subscribe(s -> System.out.printf("Acquire leadership: %s\n", s));

        Thread.sleep(1000);
    }
}
