package com.alternate.leaderelection.dynamodb;

import com.alternate.leaderelection.common.MapAdapter;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

import static com.alternate.leaderelection.dynamodb.Config.KEY_COLUMN_NAME;
import static com.alternate.leaderelection.dynamodb.Config.VALUE_COLUMN_NAME;

public class DynamoDBMap<K, V> extends MapAdapter<K, V> {

    private Class<?> k;
    private Class<?> v;

    private Table table;

    public DynamoDBMap(Class<K> k, Class<V> v, String tableName) {
        this(k, v, tableName, null);
    }

    public DynamoDBMap(Class<K> k, Class<V> v, String tableName, DynamoDB db) {
        this.k = k;
        this.v = v;
        db = (db != null) ? db : DynamoDBHelper.createLocalDB();
        this.table = DynamoDBHelper.createTable(db, tableName);
    }

    @Override
    public V get(Object key) {
        GetItemSpec getItemSpec = new GetItemSpec()
                .withPrimaryKey(KEY_COLUMN_NAME, String.valueOf(key));

        Item item = this.table.getItem(getItemSpec);

        if (item == null || !item.hasAttribute(VALUE_COLUMN_NAME)) return null;

        try {
            String valueAsString = item.getString(VALUE_COLUMN_NAME);
            return (V) SerDeHelper.getValue(valueAsString, this.v);
        } catch (IOException | ClassCastException e) {
            System.out.println("Serialization failed");
            return null;
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            String valueAsString = SerDeHelper.getValueAsString(value);
            Item item = new Item()
                    .withPrimaryKey(KEY_COLUMN_NAME, String.valueOf(key))
                    .withString(VALUE_COLUMN_NAME, valueAsString);

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item);

            this.table.putItem(putItemSpec);
            return value;
        } catch (JsonProcessingException e) {
            System.out.println("Serialization failed");
            return null;
        }
    }

    @Override
    public V remove(Object key) {
        V v = this.get(key);

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(KEY_COLUMN_NAME, String.valueOf(key));

        this.table.deleteItem(deleteItemSpec);
        return v;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        try {
            String valueAsString = SerDeHelper.getValueAsString(value);

            Item item = new Item()
                    .withPrimaryKey(KEY_COLUMN_NAME, String.valueOf(key))
                    .withString(VALUE_COLUMN_NAME, valueAsString);

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withConditionExpression("attribute_not_exists(" + KEY_COLUMN_NAME + ")");

            this.table.putItem(putItemSpec);
            return value;
        } catch (JsonProcessingException e) {
            System.out.println("Serialization failed");
            return null;
        } catch (ConditionalCheckFailedException e) {
            System.out.println("Conditional check failed: Value already exist");
            return get(key);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        try {
            String oldValueAsString = SerDeHelper.getValueAsString(oldValue);
            String newValueAsString = SerDeHelper.getValueAsString(newValue);

            Item item = new Item()
                    .withPrimaryKey(KEY_COLUMN_NAME, String.valueOf(key))
                    .withString(VALUE_COLUMN_NAME, newValueAsString);

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withExpected(new Expected(VALUE_COLUMN_NAME).eq(oldValueAsString));

            this.table.putItem(putItemSpec);
            return true;
        } catch (JsonProcessingException e) {
            System.out.println("Serialization failed");
            return false;
        } catch (ConditionalCheckFailedException e) {
            System.out.println("Conditional check failed: Value already updated");
            return false;
        }
    }
}
