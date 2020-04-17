package com.alternate.leaderelection.common;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author randilfernando
 */
public class JsonSerDe implements SerDe<String> {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static JsonSerDe INSTANCE = new JsonSerDe();

    public static JsonSerDe getInstance() {
        return INSTANCE;
    }

    @Override
    public String serialize(Object value) throws Exception {
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer || value instanceof Long) {
            return String.valueOf(value);
        } else {
            return OBJECT_MAPPER.writeValueAsString(value);
        }
    }

    @Override
    public <D> D deserialize(String serialized, Class<D> clazz) throws Exception {
        if (clazz == String.class) {
            return clazz.cast(serialized);
        } else if (clazz == Integer.class) {
            return clazz.cast(Integer.parseInt(serialized));
        } else if (clazz == Long.class) {
            return clazz.cast(Long.parseLong(serialized));
        } else {
            return OBJECT_MAPPER.readValue(serialized, clazz);
        }
    }
}
