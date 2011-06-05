package avrobase.mongodb;

import avrobase.Mutator;
import avrobase.Row;
import bagcheck.User;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import com.mongodb.DB;
import com.mongodb.Mongo;
import org.apache.avro.util.Utf8;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import static junit.framework.Assert.assertEquals;

public class MongoABTest {

  private static DB avrobasetest;
  private byte[] row;

  @BeforeClass
  public static void setup() throws UnknownHostException {
    Mongo mongo = new Mongo("localhost");
    avrobasetest = mongo.getDB("avrobasetest");
  }

  @Test
  public void putGet() throws InterruptedException {
    MongoAB<User, byte[]> userRAB = getAB();
    User user = getUser();
    userRAB.put("test".getBytes(Charsets.UTF_8), user);
    Thread.sleep(1000);
    Row<User, byte[]> test = userRAB.get("test".getBytes(Charsets.UTF_8));
    assertEquals(user, test.value);
  }

  @Test
  public void putGet2() {
    MongoAB<User, byte[]> userRAB = getAB();
    User user = getUser();
    row = "test".getBytes(Charsets.UTF_8);
    userRAB.put(row, user);
    userRAB.mutate(row, new Mutator<User>() {
      @Override
      public User mutate(User value) {
        value.firstName = $("John");
        return value;
      }
    });
    Row<User, byte[]> test = userRAB.get("test".getBytes(Charsets.UTF_8));
    user = getUser();
    user.firstName = $("John");
    assertEquals(user, test.value);
  }

  @Test
  public void rdelete() {
    MongoAB<User, byte[]> userRAB = getAB();
    row = "test".getBytes(Charsets.UTF_8);
    userRAB.delete(row);
    Row<User, byte[]> test = userRAB.get("test".getBytes(Charsets.UTF_8));
    assertEquals(null, test);
  }

  @Test
  public void testScan() {
    MongoAB<User, byte[]> userRAB = getAB();
    User user = getUser();
    long start;
    start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      userRAB.put(Ints.toByteArray(i), user);
    }
    System.out.println(System.currentTimeMillis() - start);
    start = System.currentTimeMillis();
    int total;
    start = System.currentTimeMillis();
    total = 0;
    for (Row<User, byte[]> userRow : userRAB.scan(Ints.toByteArray(50000), null)) {
      total++;
    }
    assertEquals(50000, total);
    System.out.println(System.currentTimeMillis() - start);
    start = System.currentTimeMillis();
    total = 0;
    for (Row<User, byte[]> userRow : userRAB.scan(null, Ints.toByteArray(50000))) {
      total++;
    }
    assertEquals(50000, total);
    System.out.println(System.currentTimeMillis() - start);
    start = System.currentTimeMillis();
    total = 0;
    for (Row<User, byte[]> userRow : userRAB.scan(Ints.toByteArray(25000), Ints.toByteArray(75000))) {
      total++;
    }
    assertEquals(50000, total);
    System.out.println(System.currentTimeMillis() - start);
    total = 0;
    for (Row<User, byte[]> userRow : userRAB.scan(null, null)) {
      total++;
      userRAB.delete(userRow.row);
    }
    assertEquals(100000, total);
    System.out.println(System.currentTimeMillis() - start);
    total = 0;
    for (Row<User, byte[]> userRow : userRAB.scan(null, null)) {
      total++;
    }
    assertEquals(0, total);
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

  private MongoAB<User, byte[]> getAB() {
    return new MongoAB<User, byte[]>(avrobasetest, "users", User.SCHEMA$);
  }

}