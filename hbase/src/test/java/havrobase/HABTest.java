package havrobase;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.AvroBaseFactory;
import avrobase.AvroFormat;
import avrobase.Row;
import bagcheck.GenderType;
import bagcheck.User;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.avro.Schema;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test all the public interfaces to the HAvroBase
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:21:58 PM
 */
public class HABTest {
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("profile");
  private static final byte[] TABLE = Bytes.toBytes("test_user");
  private static final byte[] SCHEMA_TABLE = Bytes.toBytes("test_schema");
  protected static final Provider<String> NULL_STRING_PROVIDER = new Provider<String>() {
    @Override
    public String get() {
      return null;
    }
  };
  private static final int INSERTS = 10000;

  static class HABModule implements Module {
    public static final HBaseAdmin admin;
    private static final Provider<Supplier<byte[]>> NULL_SUPPLIER_PROVIDER = new Provider<Supplier<byte[]>>() {
      @Override
      public Supplier<byte[]> get() {
        return null;
      }
    };

    static {
      try {
        admin = new HBaseAdmin(HBaseConfiguration.create());
      } catch (MasterNotRunningException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void configure(Binder binder) {
      binder.bind(Schema.class).toInstance(User.SCHEMA$);
      binder.bind(byte[].class).annotatedWith(Names.named("schema")).toInstance(SCHEMA_TABLE);
      binder.bind(byte[].class).annotatedWith(Names.named("table")).toInstance(TABLE);
      binder.bind(byte[].class).annotatedWith(Names.named("family")).toInstance(COLUMN_FAMILY);
      binder.bind(String.class).annotatedWith(Names.named("solr")).toProvider(NULL_STRING_PROVIDER);
      binder.bind(HAB.CreateType.class).toInstance(HAB.CreateType.SEQUENTIAL);
      binder.bind(new TypeLiteral<Supplier<byte[]>>() {}).toProvider(NULL_SUPPLIER_PROVIDER);
      binder.bind(HTablePool.class).toInstance(new HTablePool());
      binder.bind(HBaseAdmin.class).toInstance(admin);
    }
  }

  @BeforeClass
  public static void setup() {
    deleteTable(SCHEMA_TABLE);
    deleteTable(TABLE);
  }

  private static void deleteTable(byte[] tableName) {
    HTablePool pool = new HTablePool();
    HTableInterface table = null;
    try {
      table = pool.getTable(tableName);
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      for (Result r : scanner) {
        Delete delete = new Delete(r.getRow());
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = r.getNoVersionMap();
        for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> family : map.entrySet()) {
          delete.deleteFamily(family.getKey());
        }
        table.delete(delete);
      }
      Delete delete = new Delete(new byte[0]);
      table.delete(delete);
    } catch (Exception e) {
      // ignore
    } finally {
      if (table != null) pool.putTable(table);
    }
  }

  @Test
  public void testEmptyRowId() throws IOException {
    deleteTable(TABLE);
    HTablePool pool = new HTablePool();
    HTableInterface table = pool.getTable(TABLE);
    byte[] EMPTY = new byte[0];
    Put put = new Put(EMPTY);
    put.add(COLUMN_FAMILY, $("test").getBytes(), $("test").getBytes());
    table.put(put);
    Scan scan = new Scan();
    boolean found = false;
    for (Result r : table.getScanner(scan)) {
      assertFalse(found);
      found = true;
      assertEquals(0, EMPTY.length);
    }
    assertTrue(found);
    deleteTable(TABLE);
  }

  @Test
  public void testNoRow() throws AvroBaseException {
    AvroBase<User, byte[]> instance = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    Row<User, byte[]> row = instance.get(Bytes.toBytes("lukew"));
    assertEquals(null, row);
  }

  @Test
  public void testBenchmark() throws AvroBaseException, InterruptedException {
    deleteTable(TABLE);
    final AvroBase<User, byte[]> instance = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    final User saved = new User();
    saved.firstName = $("Sam");
    saved.lastName = $("Pullara");
    saved.birthday = $("1212");
    saved.gender = GenderType.MALE;
    saved.email = $("spullara@yahoo.com");
    saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
    saved.title = $("Engineer");
    saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
    saved.location = $("Los Altos, CA");
    saved.password = ByteBuffer.wrap($("").getBytes());
    long start = System.currentTimeMillis();
    final Semaphore s = new Semaphore(10);
    ExecutorService es = Executors.newCachedThreadPool();
    for (int i = 0; i < INSERTS; i++) {
      s.acquire();
      es.submit(new Runnable() {
        public void run() {
          try {
            instance.create(saved);
          } finally {
            s.release();
          }
        }
      });
    }
    s.acquire(10);
    s.release(10);
    long end = System.currentTimeMillis();
    System.out.println(INSERTS * 1000 / (end - start));
    deleteTable(TABLE);
  }

  @Test
  public void testSave() throws AvroBaseException {
    deleteTable(SCHEMA_TABLE);
    AvroBase<User, byte[]> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    User saved = new User();
    saved.firstName = $("Sam");
    saved.lastName = $("Pullara");
    saved.birthday = $("1212");
    saved.gender = GenderType.MALE;
    saved.email = $("spullara@yahoo.com");
    saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
    saved.title = $("Engineer");
    saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
    saved.location = $("Los Altos, CA");
    saved.password = ByteBuffer.wrap($("").getBytes());
    byte[] row = Bytes.toBytes("spullara");
    userHAB.put(row, saved);
    Row<User, byte[]> loaded = userHAB.get(row);
    assertEquals(saved, loaded.value);
  }

  @Test
  public void testCreateSequential() throws AvroBaseException {
    deleteTable(SCHEMA_TABLE);
    AvroBase<User, byte[]> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    User saved = new User();
    saved.firstName = $("Sam");
    saved.lastName = $("Pullara");
    saved.birthday = $("1212");
    saved.gender = GenderType.MALE;
    saved.email = $("spullara@yahoo.com");
    saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
    saved.title = $("Engineer");
    saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
    saved.location = $("Los Altos, CA");
    saved.password = ByteBuffer.wrap($("").getBytes());
    byte[] row = userHAB.create(saved);
    Row<User, byte[]> loaded = userHAB.get(row);
    assertEquals(saved, loaded.value);
    assertEquals("1", Bytes.toString(row));
  }

  @Test
  public void testSaveFail() throws AvroBaseException {
    AvroBase<User, byte[]> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    User saved = new User();
    saved.firstName = $("Sam");
    saved.lastName = $("Pullara");
    saved.birthday = $("1212");
    saved.gender = GenderType.MALE;
    saved.email = $("spullara@yahoo.com");
    saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
    saved.title = $("Engineer");
    saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
    saved.location = $("Los Altos, CA");
    saved.password = ByteBuffer.wrap($("").getBytes());
    byte[] row = Bytes.toBytes("spullara");
    assertFalse(userHAB.put(row, saved, -1));
  }

  @Test
  public void testSaveJsonFormat() throws AvroBaseException, IOException {
    AvroBase<User, byte[]> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.JSON);
    User saved = new User();
    saved.firstName = $("Sam");
    saved.lastName = $("Pullara");
    saved.birthday = $("1212");
    saved.gender = GenderType.MALE;
    saved.email = $("spullara@yahoo.com");
    saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
    saved.title = $("Engineer");
    saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
    saved.location = $("Los Altos, CA");
    saved.mobile = $("4155551212");
    saved.password = ByteBuffer.wrap($("").getBytes());
    byte[] row = Bytes.toBytes("spullara");
    userHAB.put(row, saved);
    Row<User, byte[]> loaded = userHAB.get(row);
    assertEquals(saved, loaded.value);

    HTablePool pool = new HTablePool();
    HTableInterface table = pool.getTable(TABLE);
    try {
      Get get = new Get(row);
      byte[] DATA = Bytes.toBytes("d");
      get.addColumn(COLUMN_FAMILY, DATA);
      Result result = table.get(get);
      assertTrue(Bytes.toString(result.getValue(COLUMN_FAMILY, DATA)).startsWith("{"));
    } finally {
      pool.putTable(table);
    }
  }

  @Test
  public void testAttack() throws IOException {
    for (int i = 0; i < 10; i++) {
      testSave();
      testSaveJsonFormat();
    }
  }

  @Test
  public void testScan() throws AvroBaseException, IOException {
    testSaveJsonFormat();
    AvroBase<User, byte[]> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    byte[] row = Bytes.toBytes("spullara");
    Row<User, byte[]> loaded = userHAB.get(row);
    for (Row<User, byte[]> user : userHAB.scan(row, row)) {
      assertEquals(loaded, user);
    }
  }

  @Test
  public void testSchemolution() /* will not be televised */ throws AvroBaseException, IOException {
    testSaveJsonFormat();
    byte[] row = Bytes.toBytes("spullara");
    HTablePool pool = new HTablePool();
    HTableInterface userTable = pool.getTable(TABLE);
    try {
      Get get = new Get(row);
      Result userRow = userTable.get(get);
      byte[] schemaKey = userRow.getValue(COLUMN_FAMILY, Bytes.toBytes("s"));
      HTableInterface schemaTable = pool.getTable(SCHEMA_TABLE);
      Schema actual;
      try {
        Result schemaRow = schemaTable.get(new Get(schemaKey));
        actual = Schema.parse(Bytes.toString(schemaRow.getValue(Bytes.toBytes("avro"), Bytes.toBytes("s"))));
      } finally {
        pool.putTable(schemaTable);
      }
      JsonDecoder jd = new JsonDecoder(actual, Bytes.toString(userRow.getValue(COLUMN_FAMILY, Bytes.toBytes("d"))));

      // Read it as a slightly different schema lacking a field
      InputStream stream = getClass().getResourceAsStream("/User2.json");
      Schema expected = Schema.parse(stream);

      {
        SpecificDatumReader<User> sdr = new SpecificDatumReader<User>();
        sdr.setSchema(actual);
        sdr.setExpected(expected);
        User loaded = sdr.read(null, jd);
        assertEquals("Sam", loaded.firstName.toString());
        assertEquals(null, loaded.mobile);
      }
    } finally {
      pool.putTable(userTable);
    }
  }

  @Test
  public void testAddFamily() {
    testSave();
    AvroBase<User, byte[]> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, AvroFormat.BINARY);
    Row<User, byte[]> row = userHAB.get("spullara".getBytes());
  }

  private Utf8 $(String value) {
    return new Utf8(value);
  }

}
