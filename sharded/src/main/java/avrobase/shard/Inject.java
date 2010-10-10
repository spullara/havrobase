package avrobase.shard;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Pusher annotation
 * <p/>
 * User: sam
 * Date: 10/10/10
 * Time: 11:21 AM
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.FIELD})
public @interface Inject {
  SC value();
}
