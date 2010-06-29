package avrobase;

import org.apache.avro.specific.SpecificRecord;

/**
 * Base interface for every AvroBase.
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 11:45:23 AM
 */
public interface AvroBase<T extends SpecificRecord> {
  /**
   * Return a single row
   * @param row
   * @return
   * @throws AvroBaseException
   */
  Row<T> get(byte[] row) throws AvroBaseException;

  /**
   * Save a value with an automatically generated unique key and return that key.
   * @param value
   * @return
   * @throws AvroBaseException
   */
  byte[] create(T value) throws AvroBaseException;

  /**
   * Put a row with that will retry until successfully increments the version number
   * @param row
   * @param value
   * @return
   * @throws AvroBaseException
   */
  void put(byte[] row, T value) throws AvroBaseException;

  /**
   * Put a row if the current version in the database matches the passed version.
   * @param row
   * @param value
   * @param version
   * @return
   * @throws AvroBaseException
   */
  boolean put(byte[] row, T value, long version) throws AvroBaseException;

  /**
   * Scan the database for instances.
   * @param startRow
   * @param stopRow
   * @return
   * @throws AvroBaseException
   */
  Iterable<Row<T>> scan(byte[] startRow, byte[] stopRow) throws AvroBaseException;

  /**
   * Search the set of objects in the system.
   * @param query
   * @param sort
   * @param start
   * @param rows
   * @return
   * @throws AvroBaseException
   */
  Iterable<Row<T>> search(String query, int start, int rows) throws AvroBaseException;
}
