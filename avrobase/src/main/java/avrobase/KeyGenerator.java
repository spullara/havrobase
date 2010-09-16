package avrobase;

/**
 * Generates keys.
 *
 * @param <K> key type
 */
public interface KeyGenerator<K> {
  K generate();
}
