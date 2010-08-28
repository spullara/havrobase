package avrobase.mysql;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * 64-bit keys. Note that the implementation is only generating 31-bit keys --
 * up to Integer.MAX_VALUE.
 *
 * The byte representation is a base36-encoded ASCII string which provides
 * readability and compactness.
 */
@Singleton
public class LongKeyStrategy implements KeyStrategy<Long> {
  private final Random rnd;
  private final int radix = 36;

  @Inject
  public LongKeyStrategy(Random rnd) {
    this.rnd = rnd;
  }

  @Override
  public byte[] toBytes(Long key) {
    try {
      return Long.toString(key, radix).getBytes("ASCII");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Long fromBytes(byte[] row) {
    try {
      return Long.parseLong(new String(row, "ASCII"), radix);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Long fromString(String key) {
    return Long.parseLong(key);
  }

  @Override
  public String toString(Long row) {
    return row.toString();
  }

  @Override
  public Long newKey() {
    return (long) rnd.nextInt(Integer.MAX_VALUE);
  }
}