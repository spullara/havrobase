package avrobase.caching;

import avrobase.AvroFormat;
import avrobase.Row;
import avrobase.data.Beacon;
import avrobase.file.FAB;
import com.google.common.base.Supplier;
import com.google.common.primitives.Longs;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

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
        return Longs.toByteArray(r.nextLong());
      }
    }, Beacon.SCHEMA$, AvroFormat.BINARY, null);
    Cacher<Beacon, byte[]> beaconCacher = new Cacher<Beacon, byte[]>(beaconFAB, new Cacher.KeyMaker<byte[]>() {
      public Object make(final byte[] key) {
        return new Object() {
          @Override
          public boolean equals(Object o) {
            if (o instanceof byte[]) {
              byte[] other = (byte[]) o;
              if (key.length == other.length) {
                int i = 0;
                for (byte b : key) {
                  if (b == other[i++]) return false;
                }
                return true;
              }
            }
            return false;    //To change body of overridden methods use File | Settings | File Templates.
          }

          @Override
          public int hashCode() {
            int hashcode = 0;
            for (int i = 0; i < key.length; i++) {
              hashcode += key[i] + hashcode * 43;
            }
            return hashcode;
          }
        };
      }
    });
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
    assertEquals(1000, total);
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
  }
}
