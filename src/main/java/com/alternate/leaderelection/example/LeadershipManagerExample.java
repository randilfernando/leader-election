package com.alternate.leaderelection.example;

import com.alternate.leaderelection.dynamodb.DynamoDBMap;
import com.alternate.leaderelection.leadership.LeadershipManager;
import com.alternate.leaderelection.leadership.Node;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import java.util.Map;

public class LeadershipManagerExample {

    public static void main(String[] args) throws InterruptedException {
        final AmazonDynamoDB db = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(db);

        Node candidate = new Node("node-1");
        Map<String, Node> leaders = DynamoDBMap.<String, Node>builder()
                .withDynamoDB(dynamoDB)
                .withTableName("leaders")
                .withKeyClass(String.class)
                .withValueClass(Node.class)
                .build();

        LeadershipManager leadershipManager = new LeadershipManager("group-1", candidate, leaders);

        leadershipManager.getLeaderStream().subscribe(leader -> System.out.printf("Topic leader changed: %s\n", leader.getId()));
        leadershipManager.getStatusStream().subscribe(status -> System.out.printf("Candidate status changed: %s\n", status.toString()));

        leadershipManager.start();

        leadershipManager.acquireLeadership().subscribe(s -> System.out.printf("Acquire leadership: %s\n", s));

        Thread.sleep(1000);
    }
}
