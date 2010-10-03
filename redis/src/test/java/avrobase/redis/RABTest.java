package avrobase.redis;

import avrobase.Row;
import bagcheck.User;
import com.google.common.base.Supplier;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Oct 3, 2010
 * Time: 12:21:55 PM
 */
public class RABTest {
  @Test
  public void putGet() {
    RAB<User> userRAB = getRAB();
    User user = getUser();
    userRAB.put("test", user);
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  @Test
  public void delete() {
    RAB<User> userRAB = getRAB();
    User user = getUser();
    userRAB.put("test", user);
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);
    userRAB.delete("test");
    assertEquals(null, userRAB.get("test"));
  }

  @Test
  public void putWithVersion() {
    RAB<User> userRAB = getRAB();
    User user = getUser();
    userRAB.put("test", user);
    Row<User, String> test = userRAB.get("test");
    assertEquals(user, test.value);

    assertTrue(userRAB.put("test", user, test.version));
    assertFalse(userRAB.put("test", user, test.version));
  }

  @Test
  public void create() {
    RAB<User> userRAB = getRAB();
    User user = getUser();
    String s = userRAB.create(user);
    Row<User, String> test = userRAB.get(s);
    assertEquals(user, test.value);
    userRAB.delete(s);
    assertEquals(null, userRAB.get(s));
  }

  @Test
  public void multithreaded() throws InterruptedException {
    final RAB<User> userRAB = getRAB();
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

  private RAB<User> getRAB() {
    JedisPool pool = getPool();

    return new RAB<User>(pool, 0, new Supplier<String>() {
      Random random = new SecureRandom();

      @Override
      public String get() {
        return String.valueOf(random.nextLong());
      }
    }, User.SCHEMA$);
  }

  private JedisPool getPool() {
    JedisPool pool = new JedisPool("localhost");
    pool.setResourcesNumber(100);
    pool.init();
    return pool;
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
