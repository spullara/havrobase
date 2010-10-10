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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    @Override
    public Iterable<String> scanKeys(String start, String end) throws AvroBaseException {
      final byte[] startRow = start == null ? null : keytx.toBytes(start);
      final byte[] stopRow = end == null ? null : keytx.toBytes(end);
      StringBuilder statement = new StringBuilder("SELECT row FROM ");
      statement.append(mysqlTableName);
      if (startRow != null) {
        statement.append(" WHERE row >= ?");
      }
      if (stopRow != null) {
        if (startRow == null) {
          statement.append(" WHERE row < ?");
        } else {
          statement.append(" AND row < ?");
        }
      }
      return new Query<Iterable<String>>(statement.toString()) {
        public void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
          int i = 1;
          if (startRow != null) {
            ps.setBytes(i++, startRow);
          }
          if (stopRow != null) {
            ps.setBytes(i, stopRow);
          }
        }

        public Iterable<String> execute(final ResultSet rs) throws AvroBaseException, SQLException {
          // TODO: Can't stream this yet due to database cursors
          List<String> rows = new ArrayList<String>();
          while (rs.next()) {
            byte[] row = rs.getBytes(1);
              rows.add(keytx.fromBytes(row));
          }
          return rows;
        }
      }.query();

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
  public void shard() {
    Pusher<SC> pusher = PusherBase.create(SC.class, Inject.class);
    pusher.bindInstance(SC.KEY_COMPARATOR, STRING_COMPARATOR);
    pusher.bindClass(SC.STRATEGY, ShardingStrategy.Partition.class);
    @SuppressWarnings({"unchecked"}) ShardedAvroBase<User, String> sab = pusher.create(ShardedAvroBase.class);
    sab.addShard(mab1, 1.0, false);

    int T = 1000;
    for (int i = 0; i < T; i++) {
      User user = getUser();
      String row = KEYTX.newKey();
      user.firstName = new Utf8(user.firstName.toString() + row);
      sab.put(row, user);
    }

    sab.addShard(mab2, 3.0, true);

    // Verify there are 500 in mab1 and 1500 in mab2
    int count = 0;
    for (String tRow : mab1.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T/4, count);
    for (String tRow : mab2.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T, count);

    sab.addShard(mab3, 1.0, true);

    // Verify there are 500 in mab1 and 1500 in mab2
    count = 0;
    for (String tRow : mab1.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T/5, count);
    for (String tRow : mab2.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T/5*4, count);
    for (String tRow : mab3.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T, count);

    sab.addShard(mab4, 5.0, true);
    
    count = 0;
    for (String tRow : mab1.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T/10, count);
    for (String tRow : mab2.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T/10*4, count);
    for (String tRow : mab3.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T/10*5, count);
    for (String tRow : mab4.scanKeys(null, null)) {
      count++;
    }
    assertEquals(T, count);
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
