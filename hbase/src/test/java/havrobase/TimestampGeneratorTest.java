package havrobase;

import avrobase.TimestampGenerator;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Generate unique timestamps and reverse timestamps.
 * <p/>
 * User: sam
 * Date: Jul 8, 2010
 * Time: 11:22:39 AM
 */
public class TimestampGeneratorTest {
  @Test
  public void testInvertedTimestamp() {
    TimestampGenerator generator = new TimestampGenerator();
    long last = generator.getInvertedTimestamp();
    long start = last;
    long startms = System.currentTimeMillis();
    System.out.println(last);
    for (int i = 0; i < 100000; i++) {
      long timestamp = generator.getInvertedTimestamp();
      assertTrue(timestamp < last);
      last = timestamp;
    }
    System.out.println(start - generator.getInvertedTimestamp());
    System.out.println(System.currentTimeMillis() - startms);
  }

  @Test
  public void testTimestamp() {
    TimestampGenerator generator = new TimestampGenerator();
    long last = generator.getTimestamp();
    long start = last;
    long startms = System.currentTimeMillis();
    System.out.println(last);
    for (int i = 0; i < 100000; i++) {
      long timestamp = generator.getTimestamp();
      assertTrue(timestamp > last);
      last = timestamp;
    }
    System.out.println(generator.getTimestamp() - start);
    System.out.println(System.currentTimeMillis() - startms);
  }
}
