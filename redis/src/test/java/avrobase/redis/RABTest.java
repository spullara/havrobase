package avrobase.redis;

import avrobase.Row;
import bagcheck.User;
import com.google.common.base.Supplier;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

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
    Row<User,String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  @Test
  public void delete() {
    RAB<User> userRAB = getRAB();
    User user = getUser();
    userRAB.put("test", user);
    Row<User,String> test = userRAB.get("test");
    assertEquals(user, test.value);
    userRAB.delete("test");
    assertEquals(null, userRAB.get("test"));
  }

  @Test
  public void putWithVersion() {
    RAB<User> userRAB = getRAB();
    User user = getUser();
    userRAB.put("test", user);
    Row<User,String> test = userRAB.get("test");
    assertEquals(user, test.value);

    assertTrue(userRAB.put("test", user, test.version));
    assertFalse(userRAB.put("test", user, test.version));
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
