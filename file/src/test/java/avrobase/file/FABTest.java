package avrobase.file;

import avrobase.AvroFormat;
import avrobase.ReversableFunction;
import avrobase.Row;
import bagcheck.User;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.primitives.Longs;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
}
