package avrobase.mysql;

import com.google.common.base.Function;
import org.apache.avro.specific.SpecificRecord;

public interface IndexColumn<T extends SpecificRecord, S> {
  String columnName();
  int columnType();
  String columnTypeString();
  boolean nullable();
  boolean unique();
  Function<T,S> extractKeyFn();
}
