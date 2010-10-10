package avrobase.shard;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.AvroFormat;
import avrobase.Row;
import avrobase.mysql.KeyStrategy;
import avrobase.mysql.MysqlAB;
import bagcheck.User;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import thepusher.Pusher;
import thepusher.PusherBase;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

/**
 * Test the system
 * <p/>
 * User: sam
 * Date: 10/10/10
 * Time: 11:03 AM
 */
public class ShardedAvroBaseTest {
  private static Random random = new SecureRandom();
  private static String MYSQL_SCHEMAS = "avro_schemas";
  private static final KeyStrategy<String> KEYTX = new KeyStrategy<String>() {

    @Override
    public byte[] toBytes(String key) {
      return key.getBytes();
    }

    @Override
    public String fromBytes(byte[] row) {
      return new String(row);
    }

    @Override
    public String fromString(String key) {
      return key;
    }

    @Override
    public String toString(String row) {
      return row;
    }

    @Override
    public String newKey() {
      return String.valueOf(random.nextLong());
    }
  };
  private static final Comparator<String> STRING_COMPARATOR = new Comparator<String>() {
    public int compare(String s, String s1) {
      return s.compareTo(s1);
    }
  };

  private static ShardableMysqlAB mab1;
  private static ShardableMysqlAB mab2;
  private static ShardableMysqlAB mab3;
  private static ShardableMysqlAB mab4;
  private static AvroBase[] AVRO_BASES;

  @BeforeClass
  public static void setup() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Could not find JDBC driver: " + e);
    }

    final BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl("jdbc:mysql://localhost:3306/shardedtest");
    config.setMaxConnectionsPerPartition(10);
    config.setMinConnectionsPerPartition(2);
    config.setPartitionCount(1);
    config.setLazyInit(true);
    config.setUsername("sam");
    config.setPassword("");
    BoneCPDataSource ds = new BoneCPDataSource(config);

    mab1 = new ShardableMysqlAB(ds, "user", "profile", MYSQL_SCHEMAS, User.SCHEMA$, AvroFormat.JSON, KEYTX);
    mab2 = new ShardableMysqlAB(ds, "user2", "profile", MYSQL_SCHEMAS, User.SCHEMA$, AvroFormat.JSON, KEYTX);
    mab3 = new ShardableMysqlAB(ds, "user3", "profile", MYSQL_SCHEMAS, User.SCHEMA$, AvroFormat.JSON, KEYTX);
    mab4 = new ShardableMysqlAB(ds, "user4", "profile", MYSQL_SCHEMAS, User.SCHEMA$, AvroFormat.JSON, KEYTX);
    AVRO_BASES = new AvroBase[] {mab1, mab2, mab3, mab4};

    //noinspection unchecked
    for (AvroBase<User, String> ab : AVRO_BASES) {
      for (Row<User, String> userRow : ab.scan(null, null)) {
        ab.delete(userRow.row);
      }
    }
  }

  public static class ShardableMysqlAB extends MysqlAB<User, String> implements ShardableAvroBase<User, String> {
    public ShardableMysqlAB(DataSource datasource, java.lang.String table, java.lang.String family, java.lang.String schemaTable, Schema schema, AvroFormat storageFormat, KeyStrategy<String> keytx) throws AvroBaseException {
      super(datasource, table, family, schemaTable, schema, storageFormat, keytx);
    }

    @Override
    public byte[] representation() {
      return new byte[0];
    }

    @Override
    public void init(byte[] representation) {
    }
  }

  @Test
  public void create() {
    Pusher<SC> pusher = PusherBase.create(SC.class, Inject.class);
    pusher.bindInstance(SC.KEY_COMPARATOR, STRING_COMPARATOR);
    pusher.bindClass(SC.STRATEGY, ShardingStrategy.Partition.class);
    @SuppressWarnings({"unchecked"}) ShardedAvroBase<User, String> sab = pusher.create(ShardedAvroBase.class);
    sab.addShard(mab1, 1.0, false);

    User user = getUser();
    mab1.put("test", user);
    Row<User, String> test = mab1.get("test");
    assertEquals(user, test.value);
    mab1.delete("test");
  }

  @Test
  public void create20andShard() {
    Pusher<SC> pusher = PusherBase.create(SC.class, Inject.class);
    pusher.bindInstance(SC.KEY_COMPARATOR, STRING_COMPARATOR);
    pusher.bindClass(SC.STRATEGY, ShardingStrategy.Partition.class);
    @SuppressWarnings({"unchecked"}) ShardedAvroBase<User, String> sab = pusher.create(ShardedAvroBase.class);
    sab.addShard(mab1, 1.0, false);

    for (int i = 0; i < 20; i++) {
      User user = getUser();
      String row = KEYTX.newKey();
      user.firstName = new Utf8(user.firstName.toString() + row);
      sab.put(row, user);
    }

    sab.addShard(mab2, 3.0, true);

    // Verify there are 500 in mab1 and 1500 in mab2
    int count = 0;
    for (Row<User, String> tRow : mab1.scan((String) null, null)) {
      count++;
    }
    assertEquals(5, count);
    for (Row<User, String> tRow : mab2.scan((String) null, null)) {
      count++;
    }
    assertEquals(20, count);

    sab.addShard(mab3, 1.0, true);

    // Verify there are 500 in mab1 and 1500 in mab2
    count = 0;
    for (Row<User, String> tRow : mab1.scan((String) null, null)) {
      count++;
    }
    assertEquals(4, count);
    for (Row<User, String> tRow : mab2.scan((String) null, null)) {
      count++;
    }
    assertEquals(16, count);
    for (Row<User, String> tRow : mab3.scan((String) null, null)) {
      count++;
    }
    assertEquals(20, count);

    sab.addShard(mab4, 5.0, true);
    
    count = 0;
    for (Row<User, String> tRow : mab1.scan((String) null, null)) {
      count++;
    }
    assertEquals(2, count);
    for (Row<User, String> tRow : mab2.scan((String) null, null)) {
      count++;
    }
    assertEquals(8, count);
    for (Row<User, String> tRow : mab3.scan((String) null, null)) {
      count++;
    }
    assertEquals(10, count);
    for (Row<User, String> tRow : mab4.scan((String) null, null)) {
      count++;
    }
    assertEquals(20, count);
  }

  @AfterClass
  public static void teardown() {
    //noinspection unchecked
    for (AvroBase<User, String> ab : AVRO_BASES) {
      for (Row<User, String> userRow : ab.scan(null, null)) {
        ab.delete(userRow.row);
      }
    }
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
