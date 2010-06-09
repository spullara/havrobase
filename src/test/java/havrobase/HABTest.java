package havrobase;

import bagcheck.User;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:21:58 PM
 */
public class HABTest extends TestCase {
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
    HAB.Row<User> row = instance.getRow(Bytes.toBytes("test_user"), Bytes.toBytes("profile"), Bytes.toBytes("spullara"));
    assertEquals(null, row.value);
    assertEquals(-1, row.version);
    assertEquals(Long.MAX_VALUE, row.timestamp);
  }
}
