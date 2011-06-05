package avrobase.caching;

import avrobase.AvroFormat;
import avrobase.Row;
import avrobase.data.Beacon;
import avrobase.file.FAB;
import com.google.common.base.Supplier;
import com.google.common.primitives.Longs;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Caching test.
 * <p/>
 * User: sam
 * Date: 5/10/11
 * Time: 2:12 PM
 */
public class CacherTest {
  Random r = new SecureRandom();

  @Test
  public void cachingtest() {
    FAB<Beacon, byte[]> beaconFAB = new FAB<Beacon, byte[]>("/tmp/cachingtest/beacons", "/tmp/cachingtest/schemas", new Supplier<byte[]>() {
      @Override
      public byte[] get() {
        return Longs.toByteArray(r.nextLong() % 100000);
      }
    }, Beacon.SCHEMA$, AvroFormat.BINARY, null);
    Cacher.KeyMaker<byte[]> keyMaker = new Cacher.KeyMaker<byte[]>() {
      public Object make(final byte[] key) {
        return new BytesKey(key);
      }
    };
    assertTrue(keyMaker.make(new byte[4]).equals(keyMaker.make(new byte[4])));
    CacheManager cm = CacheManager.create();
    Cache cache = new Cache("test", 10000, false, true, -1, -1);
    Cacher<Beacon, byte[]> beaconCacher = new Cacher<Beacon, byte[]>(beaconFAB, keyMaker, cache);
    cm.addCache(cache);
    int total = 0;
    for (Row<Beacon, byte[]> beaconRow: beaconCacher.scan(null, null)){
      beaconCacher.delete(beaconRow.row);
    }
    for (Row<Beacon, byte[]> beaconRow: beaconCacher.scan(null, null)){
      total++;
    }
    assertEquals(0, total);
    List<byte[]> rows = new ArrayList<byte[]>();
    for (int i = 0; i < 1000; i++) {
      Beacon beacon = new Beacon();
      beacon.browser = "asdfasdfasdfasd" + i;
      beacon.login = "adfasdfasdfasdfsfas" + i;
      beacon.useragent = "adfasdfasdfasdfadsfadsf" + i;
      beacon.parameters = new HashMap<CharSequence, CharSequence>();
      rows.add(beaconCacher.create(beacon));
    }
    for (Row<Beacon, byte[]> beaconRow: beaconCacher.scan(null, null)){
      total++;
    }
    {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 10000; i++) {
        beaconFAB.get(rows.get(r.nextInt(rows.size())));
      }
      long end = System.currentTimeMillis();
      System.out.println(end - start);
    }
    {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 10000; i++) {
        beaconCacher.get(rows.get(r.nextInt(rows.size())));
      }
      long end = System.currentTimeMillis();
      System.out.println(end - start);
    }
    {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 100000; i++) {
        beaconCacher.get(Longs.toByteArray(r.nextLong() % 100000));
      }
      long end = System.currentTimeMillis();
      System.out.println(end - start);
    }
    System.out.println(beaconCacher);
  }

}
