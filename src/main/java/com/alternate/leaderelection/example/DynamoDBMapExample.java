package com.alternate.leaderelection.example;

import com.alternate.leaderelection.dynamodb.DynamoDBMap;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import java.util.Map;

public class DynamoDBMapExample {

    public static void main(String[] args) {
        final AmazonDynamoDB db = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(db);

        Map<Integer, String> dynamoDBMap = DynamoDBMap.<Integer, String>builder()
                .withDynamoDB(dynamoDB)
                .withTableName("tempMap")
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .build();

        dynamoDBMap.put(1, "123");
        String value1 = dynamoDBMap.get(1);
        String value2 = dynamoDBMap.putIfAbsent(1, "456");
        Boolean status = dynamoDBMap.replace(1, "123", "456");
        Boolean status2 = dynamoDBMap.replace(1, "123", "456");
        String value3 = dynamoDBMap.remove(1);
        String value4 = dynamoDBMap.get(1);

        System.out.println("End");
    }
}
