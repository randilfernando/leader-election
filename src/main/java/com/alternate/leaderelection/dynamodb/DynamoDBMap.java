package com.alternate.leaderelection.dynamodb;

import com.alternate.leaderelection.common.JsonSerDe;
import com.alternate.leaderelection.common.MapAdapter;
import com.alternate.leaderelection.common.SerDe;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import static com.alternate.leaderelection.common.UnhandledException.unhandled;

/**
 * @author randilfernando
 */
public class DynamoDBMap<K, V> extends MapAdapter<K, V> {

    private final static SerDe<String> SER_DE = JsonSerDe.getInstance();

    private final DynamoDB dynamoDB;
    private final String tableName;

    private final String keyColumn;
    private final String valueColumn;

    private final Class<K> keyClass;
    private final Class<V> valueClass;

    private Table table;

    private DynamoDBMap(DynamoDBMapBuilder<K, V> builder) {
        this.dynamoDB = builder.dynamoDB;
        this.tableName = builder.tableName;

        this.keyColumn = builder.keyColumn;
        this.valueColumn = builder.valueColumn;

        this.keyClass = builder.keyClass;
        this.valueClass = builder.valueClass;

        this.table = this.createTable();
    }

    public static <K, V> DynamoDBMapBuilder<K, V> builder() {
        return new DynamoDBMapBuilder<>();
    }

    @Override
    public boolean containsKey(Object key) {
        Item item = this.getItem(key);
        return item != null;
    }

    @Override
    public V get(Object key) {
        Item item = this.getItem(key);

        if (item == null || !item.hasAttribute(this.valueColumn)) return null;

        String valueAsString = item.getString(this.valueColumn);
        return unhandled(() -> SER_DE.deserialize(valueAsString, this.valueClass));
    }

    @Override
    public V put(K key, V value) {
        Item item = unhandled(() -> this.createItem(key, value));
        this.table.putItem(item);
        return value;
    }

    @Override
    public V remove(Object key) {
        V v = this.get(key);

        String keyAsString = String.valueOf(key);
        this.table.deleteItem(this.keyColumn, keyAsString);

        return v;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        try {
            Item item = unhandled(() -> this.createItem(key, value));

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withExpected(new Expected(this.keyColumn).notExist());

            this.table.putItem(putItemSpec);
            return value;
        } catch (ConditionalCheckFailedException e) {
            return get(key);
        }
    }

    @Override
    public V replace(K key, V value) {
        try {
            Item item = unhandled(() -> this.createItem(key, value));

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withExpected(new Expected(this.keyColumn).exists());

            this.table.putItem(putItemSpec);
            return value;
        } catch (ConditionalCheckFailedException e) {
            return null;
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        try {
            String oldValueAsString = unhandled(() -> SER_DE.serialize(oldValue));
            Item item = unhandled(() -> this.createItem(key, newValue));

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withExpected(new Expected(this.valueColumn).eq(oldValueAsString));

            this.table.putItem(putItemSpec);
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        V v = this.get(key);
        return (v != null) ? v : defaultValue;
    }

    @Override
    public boolean remove(Object key, Object value) {
        String keyAsString = String.valueOf(key);

        try {
            String valueAsString = unhandled(() -> SER_DE.serialize(value));

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(this.keyColumn, keyAsString)
                    .withExpected(new Expected(this.valueColumn).eq(valueAsString));

            this.table.deleteItem(deleteItemSpec);
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public void clear() {
        this.table.delete();
        this.table = this.createTable();
    }

    @Override
    public String toString() {
        return "DynamoDB Map with key class: " + this.keyClass.getSimpleName() + " and values class: " + this.valueClass.getSimpleName();
    }

    private Item getItem(Object key) {
        String keyAsString = String.valueOf(key);

        return this.table.getItem(this.keyColumn, keyAsString);
    }

    private Item createItem(K key, V value) throws Exception {
        String keyAsString = String.valueOf(key);
        String valueAsString = SER_DE.serialize(value);

        return new Item()
                .withPrimaryKey(this.keyColumn, keyAsString)
                .withString(this.valueColumn, valueAsString);
    }

    private Table createTable() {
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement()
                        .withAttributeName(this.keyColumn)
                        .withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition()
                        .withAttributeName(this.keyColumn)
                        .withAttributeType(ScalarAttributeType.S));

        try {
            System.out.println("Create new table: " + tableName);
            return dynamoDB.createTable(createTableRequest);
        } catch (ResourceInUseException e) {
            System.out.println("Table exist get existing table");
            return dynamoDB.getTable(tableName);
        }
    }

    public final static class DynamoDBMapBuilder<K, V> {
        private String keyColumn = "keyColumn1";
        private String valueColumn = "valueColumn1";
        private Class<K> keyClass = null;
        private Class<V> valueClass = null;
        private DynamoDB dynamoDB = null;
        private String tableName = null;

        public DynamoDBMapBuilder<K, V> withKeyColumn(String keyColumn) {
            this.keyColumn = keyColumn;
            return this;
        }

        public DynamoDBMapBuilder<K, V> withValueColumn(String valueColumn) {
            this.valueColumn = valueColumn;
            return this;
        }

        public DynamoDBMapBuilder<K, V> withKeyClass(Class<K> keyClass) {
            this.keyClass = keyClass;
            return this;
        }

        public DynamoDBMapBuilder<K, V> withValueClass(Class<V> valueClass) {
            this.valueClass = valueClass;
            return this;
        }

        public DynamoDBMapBuilder<K, V> withDynamoDB(DynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public DynamoDBMapBuilder<K, V> withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DynamoDBMap<K, V> build() {
            return new DynamoDBMap<>(this);
        }
    }
}
