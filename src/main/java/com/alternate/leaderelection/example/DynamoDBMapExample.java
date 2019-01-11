package com.alternate.leaderelection.example;

import com.alternate.leaderelection.dynamodb.DynamoDBMap;

import java.util.Map;

public class DynamoDBMapExample {

    public static void main(String[] args) {
        Map<Integer, String> dynamoDBMap = new DynamoDBMap<>(Integer.class, String.class, "leaders");

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
