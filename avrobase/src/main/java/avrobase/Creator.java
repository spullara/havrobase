package avrobase;

/**
 * Creates objects.
 * User: john
 * Date: Aug 9, 2010
 * Time: 2:00:21 PM
 */
public interface Creator<T> {
  T create();
}