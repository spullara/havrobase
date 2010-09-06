package avrobase;

/**
 * Pass an implementation of Mutator to apply a change to the entity that will
 * eventually succeed barring error. Also, supports creating new values if the
 * object to be mutated does not exist yet.
 * <p/>
 * User: sam
 * Date: Aug 4, 2010
 * Time: 1:06:36 PM
 */
public interface Mutator<T> {
  /**
   * Mutates the given value.
   *
   * @param value the original value.
   * @return the mutated value, or null if there is no work to be done.
   */
  T mutate(T value);
}
