package avrobase;

import org.apache.avro.specific.SpecificRecord;

/**
 * An index with write and search interfaces.
 *
 * Often used in conjunction with an IndexingAvroBase.
 *
 * @param <K> key type
 * @param <T> entity type
 * @param <Q> query type
 *
 * @author john
 */
public interface Index<K, T extends SpecificRecord, Q> {
  /**
   * Indexes the given row. Any existing index data is removed/replaced.
   * @param row
   * @return
   * @throws AvroBaseException
   */
  void index(Row<T,K> row) throws AvroBaseException;

  /**
   * unindexes the given row.
   *
   * @param row
   * @throws AvroBaseException
   */
  void unindex(K row) throws AvroBaseException;

  /**
   * Queries and returns the matching keys
   * @param query
   * @return keys that match the query
   */
  Iterable<K> search(Q query);
}
