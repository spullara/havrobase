package avrobase.mysql;

import com.google.common.base.Charsets;

import java.security.SecureRandom;
import java.util.Random;

/**
* TODO: Edit this
* <p/>
* User: sam
* Date: 6/5/11
* Time: 12:05 PM
*/
public class LongBytesKeyStrategy implements KeyStrategy<byte[]> {

  private static Random r = new SecureRandom();

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
    return key.getBytes(Charsets.UTF_8);
  }

  @Override
  public String toString(byte[] row) {
    return new String(row, Charsets.UTF_8);
  }

  @Override
  public byte[] newKey() {
    return String.valueOf(r.nextLong()).getBytes(Charsets.UTF_8);
  }
}
