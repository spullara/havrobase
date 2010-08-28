package avrobase.mysql;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.UnsupportedEncodingException;
import java.util.Random;

@Singleton
public class IntKeyStrategy implements KeyStrategy<Integer> {
  private final Random rnd;
  private final int radix = 36;

  @Inject
  public IntKeyStrategy(Random rnd) {
    this.rnd = rnd;
  }

  @Override
  public byte[] toBytes(Integer key) {
    try {
      return Integer.toString(key, radix).getBytes("ASCII");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Integer fromBytes(byte[] row) {
    try {
      return Integer.parseInt(new String(row, "ASCII"), radix);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Integer fromString(String key) {
    return Integer.parseInt(key);
  }

  @Override
  public String toString(Integer row) {
    return row.toString();
  }

  @Override
  public Integer newKey() {
    return rnd.nextInt(Integer.MAX_VALUE);
  }
}