package avrobase.mysql.experimental;

import com.google.common.base.Function;
import org.apache.avro.specific.SpecificRecord;

/**
 * An index column.
 * @param <T> object type
 * @param <S> index value type
 */
public interface IndexColumn<T extends SpecificRecord, S> {
  /**
   * @return the name of the column for the index
   */
  String getColumnName();

  /**
   * @see java.sql.Types
   * @return the SQL type for the column.
   */
  int getColumnSqlType();

  /**
   * @return the column type for DDL purposes
   */
  String getColumnTypeString();

  /**
   * @return whether the column is allowed to be null.
   */
  boolean isNullable();

  /**
   * @return whether the column value must be unique.
   */
  boolean isUnique();


  /**
   * @return a function that produces the index value given the object.
   */
  Function<T,S> valueFunction();
}
