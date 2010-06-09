package havrobase;

import bagcheck.GenderType;
import bagcheck.User;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import junit.framework.TestCase;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:21:58 PM
 */
public class HABTest extends TestCase {
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("profile");
  private static final byte[] TABLE = Bytes.toBytes("test_user");

  static class HABModule implements Module {
    @Override
    public void configure(Binder binder) {
      binder.bind(HTablePool.class).toInstance(new HTablePool());
      try {
        binder.bind(HBaseAdmin.class).toInstance(new HBaseAdmin(new HBaseConfiguration()));
      } catch (MasterNotRunningException e) {
        e.printStackTrace();
      }
    }
  }

  public void testNoRow() throws HAvroBaseException {
    Injector injector = Guice.createInjector(new HABModule());
    HAB instance = injector.getInstance(HAB.class);
    byte[] TABLE = Bytes.toBytes("test_user");
    HAB.Row<User> row = instance.getRow(HABTest.TABLE, COLUMN_FAMILY, Bytes.toBytes("lukew"));
    assertEquals(null, row.value);
    assertEquals(-1, row.version);
    assertEquals(Long.MAX_VALUE, row.timestamp);
  }

  public void testSave() throws HAvroBaseException {
    Injector injector = Guice.createInjector(new HABModule());
    HAB<User> userHAB = injector.getInstance(HAB.class);
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
    userHAB.putRow(TABLE, COLUMN_FAMILY, row, saved);
    HAB.Row<User> loaded = userHAB.getRow(TABLE, COLUMN_FAMILY, row);
    assertEquals(saved, loaded.value);
  }

  public void testSaveFail() throws HAvroBaseException {
    Injector injector = Guice.createInjector(new HABModule());
    HAB<User> userHAB = injector.getInstance(HAB.class);
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
    assertFalse(userHAB.putRow(TABLE, COLUMN_FAMILY, row, saved, -1));
  }

  private Utf8 $(String value) {
    return new Utf8(value);
  }

}
