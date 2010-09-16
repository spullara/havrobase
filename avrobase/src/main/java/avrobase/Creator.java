package avrobase;

/**
 * Creates objects.
 * @author john
 */
public interface Creator<T> {
  /**
   * @return new T; null never allowed
   */
  T create();
}