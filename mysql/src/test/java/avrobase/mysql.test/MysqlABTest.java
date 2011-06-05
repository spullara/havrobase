package avrobase.mysql.test;

import avrobase.AvroBase;
import avrobase.AvroFormat;
import avrobase.Mutator;
import avrobase.Row;
import avrobase.mysql.LongBytesKeyStrategy;
import avrobase.mysql.MysqlAB;
import bagcheck.User;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.avro.util.Utf8;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.Assert.assertEquals;

public class MysqlABTest {

  private static final ExecutorService es = Executors.newCachedThreadPool();
  private static BoneCPDataSource dataSource;
  private byte[] row;

  @BeforeClass
  public static void setup() throws UnknownHostException {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Could not find JDBC driver: " + e);
    }

    final BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl("jdbc:mysql://localhost:3306/avrobasetest");
    config.setMaxConnectionsPerPartition(50);
    config.setPartitionCount(4);
    config.setLazyInit(true);
    config.setUsername("bagcheck");
    config.setPassword("");
    dataSource = new BoneCPDataSource(config);
  }

  @Test
  public void putGet() throws InterruptedException {
    AvroBase<User, byte[]> userRAB = getAB();
    User user = getUser();
    userRAB.put("test".getBytes(Charsets.UTF_8), user);
    Thread.sleep(1000);
    Row<User, byte[]> test = userRAB.get("test".getBytes(Charsets.UTF_8));
    assertEquals(user, test.value);
  }

  @Test
  public void putGet2() {
    AvroBase<User, byte[]> userRAB = getAB();
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
    AvroBase<User, byte[]> userRAB = getAB();
    row = "test".getBytes(Charsets.UTF_8);
    userRAB.delete(row);
    Row<User, byte[]> test = userRAB.get("test".getBytes(Charsets.UTF_8));
    assertEquals(null, test);
  }

  @Test
  public void testScan() {
    AvroBase<User, byte[]> userRAB = getAB();
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

  private MysqlAB<User, byte[]> getAB() {
    return new MysqlAB<User, byte[]>(es, dataSource, "user", "profile", "avro_schemas", User.SCHEMA$, AvroFormat.BINARY, new LongBytesKeyStrategy());
  }

}