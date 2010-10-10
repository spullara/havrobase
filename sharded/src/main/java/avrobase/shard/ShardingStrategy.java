package avrobase.shard;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.Row;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The sharding strategy controls how keys are assigned to shards
 * <p/>
 * User: sam
 * Date: 10/9/10
 * Time: 6:44 PM
 */
public interface ShardingStrategy<T extends SpecificRecord, K> {
  Shard<T, K> find(K row);

  void done(Shard<T, K> shard);

  void add(ShardableAvroBase<T, K> avroBase, double weight);

  void waitForBalance() throws InterruptedException;

  public static class Partition<T extends SpecificRecord, K> implements ShardingStrategy<T, K> {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock writeShards = lock.writeLock();
    private final Lock readShards = lock.readLock();
    private final List<PartitionedShard<T, K>> activeShards = new ArrayList<PartitionedShard<T, K>>();
    private final List<PartitionedShard<T, K>> usedShards = new ArrayList<PartitionedShard<T, K>>();
    private final Set<ShardableAvroBase<T, K>> addedAvroBase = Collections.synchronizedSet(new HashSet<ShardableAvroBase<T, K>>());

    private final Comparator<K> comparator;
    private final ExecutorService balancePool;
    private final ExecutorService es;

    public Partition(@Inject(SC.KEY_COMPARATOR) Comparator<K> comparator) {
      this.comparator = comparator;
      balancePool = Executors.newFixedThreadPool(1);
      es = Executors.newCachedThreadPool();
    }

    static class PartitionedShard<T extends SpecificRecord, K> extends Shard<T, K> {
      private K start;
      private long count;

      public PartitionedShard(ShardableAvroBase<T, K> tkAvroBase, double weight, K start) {
        super(tkAvroBase, weight);
        this.start = start;
      }
    }

    @Override
    public Shard<T, K> find(K row) {
      // TODO: convert to binary search
      readShards.lock();
      try {
        for (int i = 0; i < activeShards.size(); i++) {
          PartitionedShard<T, K> shard = activeShards.get(i);
          if (shard.start == null || comparator.compare(shard.start, row) <= 0) {
            if (i + 1 == activeShards.size() || comparator.compare(activeShards.get(i + 1).start, row) > 0) {
              synchronized (usedShards) {
                usedShards.add(shard);
              }
              return shard;
            }
          }
        }
        throw new AvroBaseException("No active shard matches row");
      } finally {
        readShards.unlock();
      }
    }

    @Override
    public void done(Shard<T, K> tkShard) {
      synchronized (usedShards) {
        usedShards.remove(tkShard);
        usedShards.notifyAll();
      }
    }

    @Override
    public void add(final ShardableAvroBase<T, K> avroBase, final double weight) {
      if (activeShards.size() == 0) {
        // The first shard handles all keys
        writeShards.lock();
        try {
          activeShards.add(new PartitionedShard<T, K>(avroBase, weight, null));
        } finally {
          writeShards.unlock();
        }
      } else {
        addedAvroBase.add(avroBase);
        balancePool.submit(new Runnable() {
          @Override
          public void run() {
            // Repartition the data given a new shard
            try {
              // Count all records
              double totalWeight = weight;
              List<Future<Long>> counts = new ArrayList<Future<Long>>();
              for (int i = 0; i < activeShards.size(); i++) {
                final PartitionedShard<T, K> shard = activeShards.get(i);
                totalWeight += shard.weight();
                final int finalI = i;
                counts.add(es.submit(new Callable<Long>() {
                  public Long call() throws Exception {
                    long count = 0;
                    K end = finalI + 1 == activeShards.size() ? null : activeShards.get(finalI + 1).start;
                    for (K row : shard.avrobase().scanKeys(shard.start, end)) {
                      // TODO: Add a method to AvroBase that gives only keys back
                      count++;
                    }
                    shard.count = count;
                    return count;
                  }
                }));
              }
              long total = 0;
              for (Future<Long> count : counts) {
                try {
                  total += count.get();
                } catch (Exception e) {
                  throw new AvroBaseException("Corrupt shard: " + e);
                }
              }
              // The new list of shards includes the new shard on the end
              List<PartitionedShard<T, K>> newShards = new ArrayList<PartitionedShard<T, K>>(activeShards);
              newShards.add(new PartitionedShard<T, K>(avroBase, weight, null));
              // Stop the world implementation
              // Lock, wait for all shards to be returned
              writeShards.lock();
              synchronized (usedShards) {
                while (usedShards.size() != 0) {
                  usedShards.wait();
                }
              }
              // Copy between shards to make new shard distributions and set the start / end key
              K current;
              for (int j = 0; j < newShards.size(); j++) {
                PartitionedShard<T, K> shard = newShards.get(j);
                PartitionedShard<T, K> nextShard = j + 1 == newShards.size() ? null : newShards.get(j + 1);
                // This is the new number of records the partition should contain
                long newcount = (long) (total * shard.weight() / totalWeight);
                current = null;
                K end = j + 1 == newShards.size() ? null : newShards.get(j + 1).start;
                for (K tRow : shard.avrobase().scanKeys(shard.start, end)) {
                  if (newcount-- <= 0) {
                    if (nextShard == null) {
                      break;
                    } else {
                      // The start is the first row that wasn't in the last shard
                      if (current != null) {
                        nextShard.start = tRow;
                        current = null;
                      }
                      // Copy all the remaining rows from this shard to the next
                      nextShard.avrobase().put(tRow, shard.avrobase().get(tRow).value);
                      // Remove them from this shard
                      shard.avrobase().delete(tRow);
                    }
                  } else {
                    current = tRow;
                  }
                }
              }
              // Set the new active shards
              activeShards.clear();
              activeShards.addAll(newShards);
            } catch (InterruptedException e) {
              throw new AvroBaseException("Shard add interrupted", e);
            } finally {
              // Notify that we are done adding this avrobase 
              synchronized (addedAvroBase) {
                addedAvroBase.remove(avroBase);
                addedAvroBase.notifyAll();
              }
              // Release the lock
              writeShards.unlock();
            }
          }
        });
      }
    }

    public void waitForBalance() throws InterruptedException {
      synchronized (addedAvroBase) {
        while (!addedAvroBase.isEmpty()) {
          addedAvroBase.wait();
        }
      }
    }
  }
}
