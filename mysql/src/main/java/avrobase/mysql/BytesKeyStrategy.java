package avrobase.mysql;

import com.google.inject.Inject;

import java.util.Random;

public class BytesKeyStrategy implements KeyStrategy<byte[]> {
  private final Random random;
  private final int numbytes;

  @Inject
  public BytesKeyStrategy(Random random, int numbytes) {
    this.random = random;
    this.numbytes = numbytes;
  }

  @Override
  public byte[] toBytes(byte[] key) {
    return key;
  }

  @Override
  public byte[] fromBytes(byte[] row) {
    return row;
  }

  @Override
  public byte[] fromString(String key) {
    return key.getBytes();
  }

  @Override
  public String toString(byte[] row) {
    return new String(row);
  }

  @Override
  public byte[] newKey() {
    byte[] bytes = new byte[numbytes];
    random.nextBytes(bytes);
    return bytes;
  }
}
