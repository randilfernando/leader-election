package com.alternate.leaderelection.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.alternate.leaderelection.dynamodb.Config.KEY_COLUMN_NAME;
import static com.alternate.leaderelection.dynamodb.Config.VALUE_COLUMN_NAME;

public class DynamoDBMap<K, V> implements Map<K, V> {

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
    public int size() {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        String keyAsString = String.valueOf(key);
        Item item = this.table.getItem(KEY_COLUMN_NAME, keyAsString);
        return item != null;
    }

    @Override
    public boolean containsValue(Object value) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        String keyAsString = String.valueOf(key);
        Item item = this.table.getItem(KEY_COLUMN_NAME, keyAsString);

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
            String keyAsString = String.valueOf(key);
            String valueAsString = SerDeHelper.getValueAsString(value);
            Item item = new Item()
                    .withPrimaryKey(KEY_COLUMN_NAME, keyAsString)
                    .withString(VALUE_COLUMN_NAME, valueAsString);

            this.table.putItem(item);
            return value;
        } catch (JsonProcessingException e) {
            System.out.println("Serialization failed");
            return null;
        }
    }

    @Override
    public V remove(Object key) {
        V v = this.get(key);

        String keyAsString = String.valueOf(key);
        this.table.deleteItem(KEY_COLUMN_NAME, keyAsString);
        return v;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.forEach(this::put);
    }

    @Override
    public void clear() {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values() {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        try {
            String keyAsString = String.valueOf(key);
            String valueAsString = SerDeHelper.getValueAsString(value);

            Item item = new Item()
                    .withPrimaryKey(KEY_COLUMN_NAME, keyAsString)
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

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        V v = this.get(key);
        return (v != null) ? v : defaultValue;
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        String keyAsString = String.valueOf(key);

        try {
            String valueAsString = SerDeHelper.getValueAsString(value);

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(KEY_COLUMN_NAME, keyAsString)
                    .withExpected(new Expected(VALUE_COLUMN_NAME).eq(valueAsString));

            this.table.deleteItem(deleteItemSpec);
            return true;
        } catch (JsonProcessingException e) {
            System.out.println("Serialization failed");
            return false;
        } catch (ConditionalCheckFailedException e) {
            System.out.println("Conditional check failed: Value not present");
            return false;
        }
    }

    @Override
    public V replace(K key, V value) {
        V v = this.get(key);
        return (v != null) ? this.put(key, value) : v;
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        // ToDo: Need to implement
        throw new UnsupportedOperationException();
    }
}
