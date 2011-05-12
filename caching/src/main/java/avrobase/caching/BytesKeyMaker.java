package avrobase.caching;

public class BytesKeyMaker implements Cacher.KeyMaker<byte[]> {
  public Object make(byte[] key) {
    return new BytesKey(key);
  }
}
