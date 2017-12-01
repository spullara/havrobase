package thepusher;

/**
 * Push values onto an object.
 * <p/>
 * User: sam
 * Date: Sep 27, 2010
 * Time: 1:23:19 PM
 */
public interface Pusher<E> {
  <T> void bindClass(E binding, Class<T> type);

  <T> void bindInstance(E binding, T instance);

  <F> F get(E binding, Class<F> type);

  <T> T create(Class<T> type);

  <T> T push(T o);
}
