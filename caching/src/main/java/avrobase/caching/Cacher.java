package avrobase.caching;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.Creator;
import avrobase.ForwardingAvroBase;
import avrobase.Mutator;
import avrobase.Row;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache the results of an avrobase and also send out messages to listneres when one is updated.
 * <p/>
 * User: sam
 * Date: 5/10/11
 * Time: 1:37 PM
 */
public class Cacher<T extends SpecificRecord, K> extends ForwardingAvroBase<T, K> {

  private AtomicLong miss = new AtomicLong(0);
  private AtomicLong hit = new AtomicLong(0);
  private AtomicLong inv = new AtomicLong(0);

  private final KeyMaker<K> keyMaker;

  public static interface Listener<K> {
    void invalidate(K row);
  }

  public static interface KeyMaker<K> {
    Object make(K key);
  }

  private ConcurrentHashMap<Object, Row<T, K>> cache = new ConcurrentHashMap<Object, Row<T, K>>();

  public Cacher(AvroBase<T, K> delegate, KeyMaker<K> keyMaker) {
    super(delegate);
    this.keyMaker = keyMaker;
  }

  private List<Listener<K>> listeners = new ArrayList<Listener<K>>();

  public void addCacheListener(Listener<K> cl) {
    listeners.add(cl);
  }

  private void invalidate(K row) {
    inv.incrementAndGet();
    for (Listener<K> listener : listeners) {
      listener.invalidate(row);
    }
  }

  @Override
  public void delete(K key) throws AvroBaseException {
    super.delete(key);
    cache.remove(keyMaker.make(key));
    invalidate(key);
  }

  @Override
  public K create(T value) throws AvroBaseException {
    K k = super.create(value);
    cache.put(keyMaker.make(k), new Row<T, K>(value, k));
    return k;
  }

  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    Object key = keyMaker.make(row);
    Row<T, K> tkRow = cache.get(key);
    if (tkRow == null) {
      miss.incrementAndGet();
      tkRow = super.get(row);
      cache.put(key, tkRow);
      invalidate(row);
    } else hit.incrementAndGet();
    return tkRow;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    Row<T, K> mutate = super.mutate(row, tMutator);
    Object key = keyMaker.make(row);
    if (mutate == null) {
      cache.remove(key);
    } else {
      cache.put(key, mutate);
    }
    invalidate(row);
    return mutate;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    Row<T, K> mutate = super.mutate(row, tMutator, tCreator);
    Object key = keyMaker.make(row);
    if (mutate == null) {
      cache.remove(key);
    } else {
      cache.put(key, mutate);
    }
    invalidate(row);
    return mutate;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    super.put(row, value);
    cache.put(keyMaker.make(row), new Row<T, K>(value, row));
    invalidate(row);
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    boolean put = super.put(row, value, version);
    cache.put(keyMaker.make(row), new Row<T, K>(value, row, version));
    invalidate(row);
    return put;
  }

  @Override
  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    final Iterable<Row<T, K>> scan = super.scan(startRow, stopRow);
    return new Iterable<Row<T, K>>() {
      @Override
      public Iterator<Row<T, K>> iterator() {
        final Iterator<Row<T, K>> iterator = scan.iterator();
        return new Iterator<Row<T, K>>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Row<T, K> next() {
            Row<T, K> next = iterator.next();
            cache.put(keyMaker.make(next.row), next);
            invalidate(next.row);
            return next;
          }

          @Override
          public void remove() {
          }
        };
      }
    };
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"hits\":").append(hit).append(",\"miss\":").append(miss).append(",\"inv\":").append(inv).append("}");
    return sb.toString();
  }
}
