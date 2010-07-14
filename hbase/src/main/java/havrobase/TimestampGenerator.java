package havrobase;

import com.google.inject.Singleton;

@Singleton
public class TimestampGenerator {
  protected static final int NS_PER_MS = 1000000;
  long startMillis = System.currentTimeMillis();
  long startMillisInNanos = startMillis * NS_PER_MS;
  long startNanos = System.nanoTime();
  long last;

  public TimestampGenerator() {
    this.last = 0;
  }

  /**
   * Return an inverted timestamp accurate to nanoseconds.
   * Never returns the same one twice given an accurate clock.
   *
   * @return
   */
  public long getInvertedTimestamp() {
    return Long.MAX_VALUE - getTimestamp();
  }

  /**
   * Return a timestamp accurate to nanoseconds. Never
   * returns the same one twice given an increasing clock. Only
   * works up to 1B/second.
   * 
   * @return
   */
  public long getTimestamp() {
    long current = startMillisInNanos + (System.nanoTime() - startNanos);
    return current <= last ? ++last : (last = current);
  }
}