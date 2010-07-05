package avrobase;

import org.apache.avro.specific.SpecificRecord;

/**
 * Row value wrapper for the associated metadata.  Wouldn't Java be great if you could add metadata to any instance
 * in a typed fashion?
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 12:07:47 PM
 */
public class Row<T extends SpecificRecord, K> {
  public final T value;
  public final K row;
  public long timestamp = Long.MAX_VALUE;
  public long version = -1;

  public Row(T value, K row) {
    this.value = value;
    this.row = row;
  }

  public Row(T value, K row, long timestamp, long version) {
    this(value, row);
    this.timestamp = timestamp;
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Row row = (Row) o;
    return !(value != null ? !value.equals(row.value) : row.value != null);
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }
}
