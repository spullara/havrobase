package avrobase.mysql;

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
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Jun 18, 2010
 * Time: 1:59:49 PM
 */
public class MABTest {

  private static final String username = "sam";
  private static final String password = "qwsBBp8P";

  private static final byte[] COLUMN_FAMILY = "profile".getBytes();
  private static final byte[] TABLE = "test_user".getBytes();
  private static final byte[] SCHEMA_TABLE = "test_schema".getBytes();

  static class MABModule implements Module {
    @Override
    public void configure(Binder binder) {
      binder.bind(byte[].class).annotatedWith(Names.named("schema")).toInstance(SCHEMA_TABLE);
      MysqlConnectionPoolDataSource poolDataSource = new MysqlConnectionPoolDataSource();
      poolDataSource.setURL("jdbc:mysql://localhost/test_bagcheck");
      poolDataSource.setUser(username);
      poolDataSource.setPassword(password);
      binder.bind(DataSource.class).toInstance(poolDataSource);
    }
  }

  @Test
  public void testSave() throws AvroBaseException {
    AvroBase<User> userHAB = getMAB();
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
    byte[] row = "spullara".getBytes();
    userHAB.put(row, saved);
    Row<User> loaded = userHAB.get(row);
    assertTrue(loaded != null);
    assertEquals(saved, loaded.value);
    userHAB.put(row, saved, loaded.version);
    assertFalse(userHAB.put(row, saved, loaded.version));
  }

  @Test
  public void testInsert() throws AvroBaseException {
    AvroBase<User> userHAB = getMAB();
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
    byte[] row = UUID.randomUUID().toString().getBytes();
    userHAB.put(row, saved, 0);
    Row<User> loaded = userHAB.get(row);
    assertTrue(loaded != null);
    assertEquals(saved, loaded.value);
    userHAB.put(row, saved, loaded.version);
    assertFalse(userHAB.put(row, saved, loaded.version));
  }

  private AvroBase<User> getMAB() {
    return AvroBaseFactory.createAvroBase(new MABModule(), MAB.class, TABLE, COLUMN_FAMILY, AvroFormat.BINARY);
  }

  @Test
  public void testSaveFail() throws AvroBaseException {
    AvroBase<User> userHAB = getMAB();
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
    byte[] row = "spullara".getBytes();
    assertFalse(userHAB.put(row, saved, -1));
  }

  @Test
  public void testScan() throws AvroBaseException, IOException {
    AvroBase<User> userHAB = getMAB();
    byte[] row = "spullara".getBytes();
    Row<User> loaded = userHAB.get(row);
    for (Row<User> user : userHAB.scan(row, null)) {
      assertEquals(loaded, user);
    }
  }

  private Utf8 $(String value) {
    return new Utf8(value);
  }
}
