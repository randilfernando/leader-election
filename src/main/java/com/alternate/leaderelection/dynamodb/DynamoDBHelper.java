package com.alternate.leaderelection.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import static com.alternate.leaderelection.dynamodb.Config.KEY_COLUMN_NAME;

public final class DynamoDBHelper {

    private DynamoDBHelper() {
    }

    public static DynamoDB createLocalDB() {
        final AmazonDynamoDB db = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
                .build();

        return new DynamoDB(db);
    }

    public static Table createTable(DynamoDB db, String tableName) {
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(tableName)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L))
                .withKeySchema(new KeySchemaElement()
                        .withAttributeName(KEY_COLUMN_NAME)
                        .withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition()
                        .withAttributeName(KEY_COLUMN_NAME)
                        .withAttributeType(ScalarAttributeType.S));

        try {
            return db.createTable(createTableRequest);
        } catch (ResourceInUseException e) {
            System.out.println("Table already present");
            return db.getTable(tableName);
        }
    }
}
