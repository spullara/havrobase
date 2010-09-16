package avrobase;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.avro.specific.SpecificRecord;

/**
 * An astract indexing AvroBase forwarder. Updated indexes whenever data is created, mutated, or deleted.
 * Handles search() itself (does not pass request to the delegate).
 *
 * @author john
 */
public abstract class IndexedAvroBase<T extends SpecificRecord, K, Q> extends ForwardingAvroBase<T, K, Q>  {
  private final Index<T,K,Q> index;

  @Inject
  public IndexedAvroBase(final AvroBase<T, K, Q> delegate, final Index<T, K, Q> index) {
    super(delegate);
    this.index = index;
  }

  @Override
  public K create(T value) throws AvroBaseException {
    final K row = delegate().create(value);
    index.index(new Row<T,K>(value, row));
    return row;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    final Row<T, K> newRow = delegate().mutate(row, tMutator);
    index.index(newRow);
    return newRow;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    final Row<T, K> newRow = delegate().mutate(row, tMutator, tCreator);
    index.index(newRow);
    return newRow;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    delegate().put(row, value);
    index.index(new Row<T,K>(value, row));
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    final boolean rv = delegate().put(row, value, version);
    if (rv) {
      index.index(new Row<T,K>(value, row));
    }
    return rv;
  }

  @Override
  public void delete(K row) throws AvroBaseException {
    delegate().delete(row);
    index.unindex(row);
  }

  /**
   * Performs the query directly. Does not delegate.
   * @param query
   * @return
   * @throws AvroBaseException
   */
  @Override
  public Iterable<Row<T, K>> search(Q query) throws AvroBaseException {
    return Iterables.transform(index.search(query), new Function<K, Row<T,K>>() {
      @Override
      public Row<T, K> apply(K from) {
        if (from == null) return null;
        return delegate().get(from);
      }
    });
  }
}