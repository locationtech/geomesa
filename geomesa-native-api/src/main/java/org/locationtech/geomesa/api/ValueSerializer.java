package org.locationtech.geomesa.api;

/**
 * Required by GeoMesaIndex in order to serialize
 * a value object into the index
 * @param <T>
 */
public interface ValueSerializer<T> {

    byte[] toBytes(T t);

    T fromBytes(byte[] bytes);
}
