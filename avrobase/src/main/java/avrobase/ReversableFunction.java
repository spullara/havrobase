package avrobase;

import com.google.common.base.Function;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Sep 16, 2010
 * Time: 1:45:29 PM
 */
public interface ReversableFunction<K, S> extends Function<K, S> {
  public S apply(K k);
  public K unapply(S s);
}
