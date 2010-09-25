package avrobase.mysql;

import org.apache.avro.specific.SpecificRecord;

import java.sql.Types;

public abstract class Reference<T extends SpecificRecord> implements IndexColumn<T, Long> {
  @Override
  public int columnType() {
    return Types.BIGINT;
  }

  @Override
  public String columnTypeString() {
    return "BIGINT";
  }

  @Override
  public boolean nullable() {
    return false;
  }

  @Override
  public boolean unique() {
    return false;
  }
}
