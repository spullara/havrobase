package avrobase;

import org.apache.avro.specific.SpecificRecord;

/**
 * Base interface for every AvroBase.
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 11:45:23 AM
 */
public interface AvroBase<T extends SpecificRecord, K, Q> {
  /**
   * Return a single row
   * @param row
   * @return
   * @throws AvroBaseException
   */
  Row<T, K> get(K row) throws AvroBaseException;

  /**
   * Save a value with an automatically generated unique key and return that key.
   * @param value
   * @return
   * @throws AvroBaseException
   */
  K create(T value) throws AvroBaseException;

  /**
   * Put a row with that will retry until successfully increments the version number
   * @param row
   * @param value
   * @return
   * @throws AvroBaseException
   */
  void put(K row, T value) throws AvroBaseException;

  /**
   * Put a row if the current version in the database matches the passed version.
   * @param row
   * @param value
   * @param version
   * @return
   * @throws AvroBaseException
   */
  boolean put(K row, T value, long version) throws AvroBaseException;

  /**
   * Delete the row.
   * @param row
   * @throws AvroBaseException
   */
  void delete(K row) throws AvroBaseException;

  /**
   * Scan the database for instances.
   * @param startRow
   * @param stopRow
   * @return
   * @throws AvroBaseException
   */
  Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException;

  /**
   * Search the set of objects in the system.
   * @param query
   * @param sort
   * @param start
   * @param rows
   * @return
   * @throws AvroBaseException
   */
  Iterable<Row<T, K>> search(Q query) throws AvroBaseException;

  /**
   * Mutate the object and put it back in the AvroBase until success.
   * @param row
   * @param mutator
   * @return the mutated value.
   * @throws AvroBaseException
   */
  Row<T, K> mutate(K row, Mutator<T> mutator) throws AvroBaseException;

  /**
   * Mutate the object and put it back in the AvroBase until success. Same as mutate(K,Mutator<T>),
   * except this will call the given creator if the specified row does not exist. If the row
   * does not exist, the creator will create a new object, and then the mutator will mutate and
   * put it.
   *
   * @param row
   * @param mutator
   * @param creator If the object to mutate does not exist, and a creator is provided, the
   * creator will be used to create a new instance. Mutate will then be used on the newly
   * created instance.
   * @return the mutated value. Returns null only if the row was not found and a creator
   * was not provided (or the creator returned null).
   * @throws AvroBaseException
   */
  Row<T, K> mutate(K row, Mutator<T> mutator, Creator<T> creator) throws AvroBaseException;
}
