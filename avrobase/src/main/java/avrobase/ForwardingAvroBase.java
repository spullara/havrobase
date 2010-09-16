package avrobase;

import com.google.common.collect.ForwardingObject;
import org.apache.avro.specific.SpecificRecord;

/**
 * A forwarding AvroBase. Useful for composition of AvroBases. See IndexingAvroBase for an example.
 *
 * @param <T>
 * @param <K>
 * @param <Q>
 */
public abstract class ForwardingAvroBase<T extends SpecificRecord, K> extends ForwardingObject implements AvroBase<T, K> {
  private final AvroBase<T, K> delegate;

  public ForwardingAvroBase(AvroBase<T, K> delegate) {
    this.delegate = delegate;
  }

  public Row<T, K> get(K row) throws AvroBaseException {
    return delegate.get(row);
  }

  public K create(T value) throws AvroBaseException {
    return delegate.create(value);
  }

  public void put(K row, T value) throws AvroBaseException {
    delegate.put(row, value);
  }

  public boolean put(K row, T value, long version) throws AvroBaseException {
    return delegate.put(row, value, version);
  }

  public void delete(K row) throws AvroBaseException {
    delegate.delete(row);
  }

  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    return delegate.scan(startRow, stopRow);
  }

  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    return delegate.mutate(row, tMutator);
  }

  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    return delegate.mutate(row, tMutator, tCreator);
  }

  @Override
  protected AvroBase<T,K> delegate() {
    return delegate;
  }
}
