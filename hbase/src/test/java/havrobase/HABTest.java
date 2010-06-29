package havrobase;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.AvroBaseFactory;
import avrobase.AvroFormat;
import avrobase.Row;
import bagcheck.GenderType;
import bagcheck.User;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.avro.Schema;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

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

  static class HABModule implements Module {
    public static final HBaseAdmin admin;

    static {
      try {
        admin = new HBaseAdmin(new HBaseConfiguration());
      } catch (MasterNotRunningException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void configure(Binder binder) {
      binder.bind(byte[].class).annotatedWith(Names.named("schema")).toInstance(SCHEMA_TABLE);
      binder.bind(String.class).annotatedWith(Names.named("solr")).toInstance("http://localhost:8983/solr/user");
      binder.bind(HAB.CreateType.class).toInstance(HAB.CreateType.SEQUENTIAL);
      binder.bind(HTablePool.class).toInstance(new HTablePool());
      binder.bind(HBaseAdmin.class).toInstance(admin);
    }
  }

  @BeforeClass
  public static void setup() {
    deleteSchemaTable();
    deleteUserTable();
  }

  private static void deleteUserTable() {
    try {
      HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
      admin.disableTable(TABLE);
      admin.deleteTable(TABLE);
    } catch (IOException e) {
    }
  }

  private static void deleteSchemaTable() {
    try {
      HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
      admin.disableTable(SCHEMA_TABLE);
      admin.deleteTable(SCHEMA_TABLE);
    } catch (IOException e) {
    }
  }

  @Test
  public void testNoRow() throws AvroBaseException {
    AvroBase<User> instance = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, TABLE, COLUMN_FAMILY, AvroFormat.BINARY);
    Row<User> row = instance.get(Bytes.toBytes("lukew"));
    assertEquals(null, row);
  }

  @Test
  public void testSave() throws AvroBaseException {
    deleteSchemaTable();
    AvroBase<User> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, TABLE, COLUMN_FAMILY, AvroFormat.BINARY);
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
    Row<User> loaded = userHAB.get(row);
    assertEquals(saved, loaded.value);
  }

  @Test
  public void testSaveFail() throws AvroBaseException {
    AvroBase<User> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, TABLE, COLUMN_FAMILY, AvroFormat.BINARY);
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
    AvroBase<User> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, TABLE, COLUMN_FAMILY, AvroFormat.JSON);
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
    Row<User> loaded = userHAB.get(row);
    assertEquals(saved, loaded.value);

    HTablePool pool = new HTablePool();
    HTable table = pool.getTable(TABLE);
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
  public void testScan() throws AvroBaseException, IOException {
    testSaveJsonFormat();
    AvroBase<User> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, TABLE, COLUMN_FAMILY, AvroFormat.BINARY);
    byte[] row = Bytes.toBytes("spullara");
    Row<User> loaded = userHAB.get(row);
    for (Row<User> user : userHAB.scan(row, row)) {
      assertEquals(loaded, user);
    }
  }

  @Test
  public void testSchemolution() /* will not be televised */ throws AvroBaseException, IOException {
    testSaveJsonFormat();
    byte[] row = Bytes.toBytes("spullara");
    HTablePool pool = new HTablePool();
    HTable userTable = pool.getTable(TABLE);
    try {
      Get get = new Get(row);
      Result userRow = userTable.get(get);
      byte[] schemaKey = userRow.getValue(COLUMN_FAMILY, Bytes.toBytes("s"));
      HTable schemaTable = pool.getTable(SCHEMA_TABLE);
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
    AvroBase<User> userHAB = AvroBaseFactory.createAvroBase(new HABModule(), HAB.class, TABLE, "profile2".getBytes(), AvroFormat.JSON);
    Row<User> row = userHAB.get("spullara".getBytes());
  }

  private Utf8 $(String value) {
    return new Utf8(value);
  }

}
