package avrobase;

/**
 * Pass an implementation of Mutator to apply a change to the entity that will
 * eventually succeed barring error.
 * <p/>
 * User: sam
 * Date: Aug 4, 2010
 * Time: 1:06:36 PM
 */
public interface Mutator<T> {
  T mutate(T value);
}
