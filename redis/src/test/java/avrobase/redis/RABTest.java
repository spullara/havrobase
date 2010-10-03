package avrobase.redis;

import avrobase.Row;
import bagcheck.User;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.nio.ByteBuffer;

import static junit.framework.Assert.assertEquals;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Oct 3, 2010
 * Time: 12:21:55 PM
 */
public class RABTest {
  @Test
  public void setGet() {
    JedisPool pool = new JedisPool("localhost");
    pool.init();
    
    RAB<User> userRAB = new RAB<User>(pool, 0, User.SCHEMA$);
    User user = new User();
    user.email = $("spullara@yahoo.com");
    user.firstName = $("Sam");
    user.lastName = $("Pullara");
    user.image = $("");
    user.password = ByteBuffer.allocate(0);
    userRAB.put("test", user);
    Row<User,String> test = userRAB.get("test");
    assertEquals(user, test.value);
  }

  Utf8 $(String s) {
    return new Utf8(s);
  }
}
