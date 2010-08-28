package avrobase.mysql;

public interface KeyStrategy<K> {
  byte[] toBytes(K key);
  K fromBytes(byte[] row);
  K fromString(String key);
  String toString(K row);
  K newKey();
}