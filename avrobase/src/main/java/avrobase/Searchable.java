package avrobase;

import org.apache.avro.specific.SpecificRecord;

public interface Searchable<T extends SpecificRecord, K, Q> {
  Iterable<Row<T, K>> search(Q query) throws AvroBaseException;
  Row<T, K> lookup(Q query) throws AvroBaseException;
}