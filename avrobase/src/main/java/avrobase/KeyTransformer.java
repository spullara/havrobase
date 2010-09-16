package avrobase;

/**
 * Transforms keys from the native type to various and sundry common types.
 *
 * @author john
 */
public interface KeyTransformer<K> {
  String toString(K key);
  K fromString(String keyString);
  byte[] toBytes(K key);
  K fromBytes(byte[] keyBytes);
}
