package avrobase.caching;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.Creator;
import avrobase.ForwardingAvroBase;
import avrobase.Mutator;
import avrobase.Row;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import org.apache.avro.specific.SpecificRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache the results of an avrobase and also send out messages to listeners when one is updated.
 * <p/>
 * User: sam
 * Date: 5/10/11
 * Time: 1:37 PM
 */
public class Cacher<T extends SpecificRecord, K> extends ForwardingAvroBase<T, K> {

  private final KeyMaker<K> keyMaker;

  public static interface Listener<K> {
    void invalidate(K row);
  }

  public static interface KeyMaker<K> {
    Object make(K key);
  }

  private Cache cache;

  public Cacher(AvroBase<T, K> delegate, KeyMaker<K> keyMaker, Cache cache) {
    super(delegate);
    this.keyMaker = keyMaker;
    this.cache = cache;
  }

  private List<Listener<K>> listeners = new ArrayList<Listener<K>>();

  public void addCacheListener(Listener<K> cl) {
    listeners.add(cl);
  }

  private void invalidate(K row) {
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
    Row<T, K> tkRow = new Row<T, K>(value, k);
    cache.put(new Element(keyMaker.make(k), tkRow));
    return k;
  }

  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    Object key = keyMaker.make(row);
    Element element = cache.get(key);
    Row<T, K> tkRow;
    if (element == null) {
      tkRow = super.get(row);
      cache.put(new Element(key, tkRow));
      invalidate(row);
    } else {
      // TODO: until we offer immutable rows, clone the result
      Serializable value = element.getValue();
      tkRow = value == null ? null : ((Row<T, K>) value).clone();
    }
    return tkRow;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    Row<T, K> mutate = super.mutate(row, tMutator);
    Object key = keyMaker.make(row);
    if (mutate == null) {
      cache.remove(key);
    } else {
      cache.put(new Element(key, mutate));
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
      cache.put(new Element(key, mutate));
    }
    invalidate(row);
    return mutate;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    super.put(row, value);
    cache.put(new Element(keyMaker.make(row), new Row<T, K>(value, row)));
    invalidate(row);
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    boolean put = super.put(row, value, version);
    cache.put(new Element(keyMaker.make(row), new Row<T, K>(value, row, version)));
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
            cache.put(new Element(keyMaker.make(next.row), next));
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

  public void invalidate() {
    cache.flush();
  }
}
