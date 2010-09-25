package avrobase;

import org.apache.avro.specific.SpecificRecord;

/**
 * @author john
 */
public abstract class MultiIndexedAvroBase<T extends SpecificRecord, K> extends ForwardingAvroBase<T, K>  {
  protected final Index<T,K,?>[] indexes;

  public MultiIndexedAvroBase(AvroBase<T, K> delegate, Index<T, K, ?>... indexes) {
    super(delegate);
    this.indexes = indexes;
  }

  @Override
  public K create(T value) throws AvroBaseException {
    final K row = delegate().create(value);
    for (Index<T,K,?> index : indexes) {
      index.index(new Row<T,K>(value, row));
    }
    return row;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    final Row<T, K> newRow = delegate().mutate(row, tMutator);
    for (Index<T,K,?> index : indexes) {
      index.index(newRow);
    }
    return newRow;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    final Row<T, K> newRow = delegate().mutate(row, tMutator, tCreator);
    for (Index<T,K,?> index : indexes) {
      index.index(newRow);
    }
    return newRow;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    delegate().put(row, value);
    for (Index<T,K,?> index : indexes) {
      index.index(new Row<T,K>(value, row));
    }
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    final boolean rv = delegate().put(row, value, version);
    if (rv) {
      for (Index<T,K,?> index : indexes) {
        index.index(new Row<T,K>(value, row));
      }
    }
    return rv;
  }

  @Override
  public void delete(K row) throws AvroBaseException {
    delegate().delete(row);
    for (Index<T,K,?> index : indexes) {
      index.unindex(row);
    }
  }
}