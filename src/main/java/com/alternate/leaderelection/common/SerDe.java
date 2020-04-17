package com.alternate.leaderelection.common;

/**
 * @author randilfernando
 */
public interface SerDe<S> {

    S serialize(Object value) throws Exception;

    <D> D deserialize(S serialized, Class<D> clazz) throws Exception;
}
