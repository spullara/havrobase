package avrobase.mysql;

import avrobase.TimestampGenerator;
import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

/**
* Create a get that is at NOW but inverted so it is at the beginning of the range scan.
* <p/>
* User: sam
* Date: 6/5/11
* Time: 12:05 PM
*/
public class InvertedTimestampKeyStrategy implements KeyStrategy<byte[]> {
  private static TimestampGenerator tg = new TimestampGenerator();

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
    return Longs.toByteArray(tg.getInvertedTimestamp());
  }
}
