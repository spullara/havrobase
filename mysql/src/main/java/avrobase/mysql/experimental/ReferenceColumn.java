package avrobase.mysql.experimental;

import org.apache.avro.specific.SpecificRecord;

import java.sql.Types;

/**
 * A index column that is a reference to another entity.
 * @param <T>
 */
public abstract class ReferenceColumn<T extends SpecificRecord> implements IndexColumn<T, Long> {
  @Override
  public int getColumnSqlType() {
    return Types.BIGINT;
  }

  @Override
  public String getColumnTypeString() {
    return "BIGINT";
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public boolean isUnique() {
    return false;
  }
}
