package avrobase;

/**
 * Hbase-compatible
 * @user john
 */
public class BytesKeyTransformer implements KeyTransformer<byte[]> {
  @Override
  public String toString(byte[] key) {
    return Bytes.utf8String(key);
  }

  @Override
  public byte[] fromString(String keyString) {
    return Bytes.utf8Bytes(keyString);
  }

  @Override
  public byte[] toBytes(byte[] key) {
    return key;
  }

  @Override
  public byte[] fromBytes(byte[] keyBytes) {
    return keyBytes;
  }
}
