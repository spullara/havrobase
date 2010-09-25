package avrobase;

import org.apache.avro.specific.SpecificRecord;

public class FilteringAvroBase<T extends SpecificRecord, K> extends ForwardingAvroBase<T,K> {
  public FilteringAvroBase(AvroBase<T, K> delegate) {
    super(delegate);
  }


}
