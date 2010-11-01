package avrobase.file;

import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import bagcheck.User;
import com.google.common.base.Supplier;
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
    FAB<User> userRAB = getFAB("");
    User user = getUser();
    userRAB.put("test", user);
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  @Test
  public void putGet2() {
    FAB<User> userRAB = getFAB("");
    User user = getUser();
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  private FAB<User> getFAB(String base) {
    return new FAB<User>(base + "/tmp/users", base + "/tmp/schemas", new Supplier<String>() {
      Random random = new SecureRandom();

      @Override
      public String get() {
        return String.valueOf(random.nextLong());
      }
    }, User.SCHEMA$, AvroFormat.BINARY);
  }

  @Test
  public void multithreadedContention() throws InterruptedException, IOException {
    final FAB<User> userRAB = getFAB("");
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
  public void multithreadedContention2() throws InterruptedException, IOException {
    final FAB<User> userRAB = getFAB("/Volumes/Data");
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
