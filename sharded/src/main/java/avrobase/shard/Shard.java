package avrobase.shard;

import avrobase.AvroBase;
import org.apache.avro.specific.SpecificRecord;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 10/10/10
 * Time: 9:59 AM
 */
public class Shard<T extends SpecificRecord, K> {
  private final AvroBase<T, K> avroBase;
  private final double weight;

  public Shard(AvroBase<T, K> avroBase, double weight) {
    this.avroBase = avroBase;
    this.weight = weight;
  }

  public AvroBase<T,K> avrobase() {
    return avroBase;
  }

  public double weight() {
    return weight;
  }
}
