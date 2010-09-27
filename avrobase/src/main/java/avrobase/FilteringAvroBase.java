package avrobase;

import org.apache.avro.specific.SpecificRecord;

public class FilteringAvroBase<T extends SpecificRecord, K> extends ForwardingAvroBase<T,K> {
  public FilteringAvroBase(AvroBase<T, K> delegate) {
    super(delegate);
  }

  @Override
  public K create(T value) throws AvroBaseException {
    return super.create(value);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    super.put(row, value);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    return super.put(row, value, version);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void delete(K row) throws AvroBaseException {
    super.delete(row);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    return super.scan(startRow, stopRow);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    return super.mutate(row, tMutator);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    return super.mutate(row, tMutator, tCreator);    //To change body of overridden methods use File | Settings | File Templates.
  }
}
