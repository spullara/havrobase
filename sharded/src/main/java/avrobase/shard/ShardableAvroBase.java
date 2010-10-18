package avrobase.shard;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import org.apache.avro.specific.SpecificRecord;

/**
 * An avrobase that can be serialized and reconstituted
 * <p/>
 * User: sam
 * Date: 10/10/10
 * Time: 12:06 PM
 */
public interface ShardableAvroBase<T extends SpecificRecord, K> extends AvroBase<T, K> {
  byte[] representation() throws AvroBaseException;
  void init(byte[] representation) throws AvroBaseException;
  Iterable<K> scanKeys(K start, K end) throws AvroBaseException;
}
