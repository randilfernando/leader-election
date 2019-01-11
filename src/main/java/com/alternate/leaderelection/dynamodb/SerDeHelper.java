package com.alternate.leaderelection.dynamodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class SerDeHelper {
    private final static ObjectMapper objectMapper = new ObjectMapper();

    private SerDeHelper() {
    }

    public static String getValueAsString(Object value) throws JsonProcessingException {
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer || value instanceof Long) {
            return String.valueOf(value);
        } else {
            return objectMapper.writeValueAsString(value);
        }
    }

    public static <T> Object getValue(String valueAsString, Class<T> c) throws IOException {
        if (c == String.class) {
            return valueAsString;
        } else if (c == Integer.class) {
            return Integer.parseInt(valueAsString);
        } else if (c == Long.class) {
            return Long.parseLong(valueAsString);
        } else {
            return objectMapper.readValue(valueAsString, c);
        }
    }
}
