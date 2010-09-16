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
}
