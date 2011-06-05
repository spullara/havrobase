package avrobase.file;

import avrobase.AvroFormat;
import avrobase.ReversableFunction;
import avrobase.Row;
import bagcheck.Beacon;
import bagcheck.User;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.primitives.Longs;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.ProviderCredentials;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static junit.framework.Assert.assertEquals;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 10/10/10
 * Time: 10:09 PM
 */
public class FABTest {
  @Test
  public void putGet() {
    FAB<User, String> userRAB = getFAB("");
    User user = getUser();
    userRAB.put("test", user);
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  @Test
  public void putGet2() {
    FAB<User, String> userRAB = getFAB("");
    User user = getUser();
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  private FAB<User, String> getFAB(String base) {
    return new FAB<User, String>(base + "/tmp/users", base + "/tmp/schemas", new Supplier<String>() {
      Random random = new SecureRandom();

      @Override
      public String get() {
        return String.valueOf(random.nextLong());
      }
    }, User.SCHEMA$, AvroFormat.BINARY, new ReversableFunction<String, byte[]>() {

      @Override
      public byte[] apply(String s) {
        return s.getBytes(Charsets.UTF_8);
      }

      @Override
      public String unapply(byte[] bytes) {
        return new String(bytes, Charsets.UTF_8);
      }
    });
  }

  @Test
  public void multithreadedContention() throws InterruptedException, IOException {
    final FAB<User, String> userRAB = getFAB("");
    multithreadedtest(userRAB);
  }

  @Test
  public void multithreadedContention2() throws InterruptedException, IOException {
    final FAB<User, String> userRAB = getFAB("/Volumes/Data");
    multithreadedtest(userRAB);
  }

  private void multithreadedtest(final FAB<User, String> userRAB) throws InterruptedException {
    User user = getUser();
    final List<String> keys = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      keys.add(userRAB.create(user));
    }
    final Random r = new SecureRandom();
    ExecutorService es = Executors.newCachedThreadPool();
    final AtomicInteger failures = new AtomicInteger(0);
    final AtomicInteger total = new AtomicInteger(0);
    final Semaphore s = new Semaphore(100);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 20; i++) {
      s.acquireUninterruptibly();
      es.submit(new Runnable() {
        @Override
        public void run() {
          try {
            for (int i = 0; i < 500; i++) {
              total.incrementAndGet();
              String key = keys.get(r.nextInt(keys.size()));
              Row<User, String> userStringRow = userRAB.get(key);
              if (!userRAB.put(key, userStringRow.value, userStringRow.version)) {
                failures.incrementAndGet();
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            s.release();
          }
        }
      });
    }
    s.acquireUninterruptibly(100);
    es.shutdown();
    es.awaitTermination(1000, TimeUnit.SECONDS);
    System.out.println(failures + " out of " + total + " in " + (System.currentTimeMillis() - start) + "ms");
  }

  @Test
  public void scantest() {
    final FAB<User, String> userRAB = getFAB("/Volumes/Data");
    int total = 0;
    for (Row<User, String> userStringRow : userRAB.scan(null, null)) {
      total++;
    }
    System.out.println(total);
  }

  @Test
  public void halfscantest() {
    final FAB<User, String> userRAB = getFAB("/Volumes/Data");
    int total = 0;
    for (Row<User, String> userStringRow : userRAB.scan("0", null)) {
      total++;
    }
    System.out.println(total);
  }

  private User getUser() {
    User user = new User();
    user.email = $("spullara@yahoo.com");
    user.firstName = $("Sam");
    user.lastName = $("Pullara");
    user.image = $("");
    user.password = ByteBuffer.allocate(0);
    return user;
  }

  Utf8 $(String s) {
    return new Utf8(s);
  }

  public void testBeaconLoadAndScan() throws IOException, S3ServiceException, InterruptedException {
    final FAB<Beacon, byte[]> beaconFAB = new FAB<Beacon, byte[]>("/Volumes/Data/tmp/beacons", "/Volumes/Data/tmp/schemas", new Supplier<byte[]>() {
      Random r = new SecureRandom();
      @Override
      public byte[] get() {
        return Longs.toByteArray(r.nextLong());
      }
    }, Beacon.SCHEMA$, AvroFormat.BINARY, null);
    Properties p = new Properties();
    p.load(getClass().getResourceAsStream("creds.properties"));
    ProviderCredentials pc = new AWSCredentials(p.getProperty("AWS_ACCESS_KEY_ID"), p.getProperty("AWS_SECRET_ACCESS_KEY"));
    final RestS3Service s3 = new RestS3Service(pc);
    S3Object[] s3Objects = s3.listObjects("com.bagcheck.archive", "beacons/", null);
    ExecutorService es = Executors.newCachedThreadPool();
    List<Callable<Void>> callables = new ArrayList<Callable<Void>>();
    for (final S3Object s3Object : s3Objects) {
      if (!s3Object.getName().equals("beacons/")) {
        callables.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            S3Object object = s3.getObject(s3Object.getBucketName(), s3Object.getName());
            DataInputStream dis = new DataInputStream(new GZIPInputStream(object.getDataInputStream()));
            byte[] bytes = new byte[dis.readInt()];
            dis.readFully(bytes);
            byte[] valuebytes = new byte[1024];
            Schema writerSchema = Schema.parse(new ByteArrayInputStream(bytes));
            while(dis.readBoolean()) {
              byte[] row = new byte[dis.readInt()];
              dis.readFully(row);
              int len = dis.readInt();
              if (len > valuebytes.length) {
                valuebytes = new byte[len];
              }
              dis.readFully(valuebytes, 0, len);
              DecoderFactory decoderFactory = new DecoderFactory();
              Decoder d = decoderFactory.binaryDecoder(new ByteArrayInputStream(valuebytes, 0, len), null);
              // Read the data
              SpecificDatumReader<Beacon> sdr = new SpecificDatumReader<Beacon>(writerSchema, Beacon.SCHEMA$);
              Beacon read = sdr.read(null, d);
              beaconFAB.put(row, read);
            }
            return null;
          }
        });
      }
    }
    es.invokeAll(callables);
    es.shutdownNow();
  }

  @Test
  public void testBeaconScan() {
    final FAB<Beacon, byte[]> beaconFAB = new FAB<Beacon, byte[]>("/Volumes/Data/tmp/beacons", "/Volumes/Data/tmp/schemas", new Supplier<byte[]>() {
      Random r = new SecureRandom();
      @Override
      public byte[] get() {
        return Longs.toByteArray(r.nextLong());
      }
    }, Beacon.SCHEMA$, AvroFormat.BINARY, null);
    int total = 0;
    for (Row<Beacon, byte[]> beaconRow : beaconFAB.scan(null, null)) {
      total++;
    }
    System.out.println(total);
  }
}
