package avrobase.shard;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.Creator;
import avrobase.Mutator;
import avrobase.Row;
import org.apache.avro.specific.SpecificRecord;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static avrobase.shard.SC.STRATEGY;

/**
 * Implementation of sharding for non-indexed avrobases.
 * <p/>
 * User: sam
 * Date: 10/9/10
 * Time: 6:22 PM
 */
public class ShardedAvroBase<T extends SpecificRecord, K> implements AvroBase<T, K>  {

  private final ShardingStrategy<T, K> strategy;

  public ShardedAvroBase(@Inject(STRATEGY) ShardingStrategy<T, K> strategy) {
    this.strategy = strategy;
  }

  // Delegating

  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    Shard<T, K> shard = strategy.find(row);
    try {
      AvroBase<T, K> ab = shard.avrobase();
      return ab.get(row);
    } finally {
      strategy.done(shard);
    }
  }

  @Override
  public K create(T value) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    Shard<T, K> shard = strategy.find(row);
    try {
      AvroBase<T, K> ab = shard.avrobase();
      ab.put(row, value);
    } finally {
      strategy.done(shard);
    }
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    Shard<T, K> shard = strategy.find(row);
    try {
      AvroBase<T, K> ab = shard.avrobase();
      return ab.put(row, value, version);
    } finally {
      strategy.done(shard);
    }
  }

  @Override
  public void delete(K row) throws AvroBaseException {
    Shard<T, K> shard = strategy.find(row);
    try {
      AvroBase<T, K> ab = shard.avrobase();
      ab.delete(row);
    } finally {
      strategy.done(shard);
    }
  }

  @Override
  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    throw new NotImplementedException();
  }

  // Sharding
  public void addShard(ShardableAvroBase<T, K> avroBase, double weight, boolean wait) throws AvroBaseException {
    strategy.add(avroBase, weight);
    if (wait) {
      try {
        strategy.waitForBalance();
      } catch (InterruptedException e) {
        throw new AvroBaseException("Shard balancing interrupted", e);
      }
    }
  }

}
