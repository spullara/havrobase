package avrobase.memcached;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.AvroBaseFactory;
import avrobase.AvroFormat;
import avrobase.Row;
import bagcheck.GenderType;
import bagcheck.User;
import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Memcached client test
 * <p/>
 * User: sam
 * Date: Jun 23, 2010
 * Time: 12:49:23 PM
 */
public class MABTest {

  static class MABModule implements Module {
    static {
      String[] serverlist = {"localhost:11211"};

      SockIOPool pool = SockIOPool.getInstance();
      pool.setServers(serverlist);
      pool.initialize();
    }

    @Override
    public void configure(Binder binder) {
      binder.bind(MemCachedClient.class).toInstance(new MemCachedClient(true));
      binder.bind(Schema.class).toInstance(User.SCHEMA$);
      binder.bind(String.class).annotatedWith(Names.named("schema")).toInstance("test_schema");
      binder.bind(String.class).annotatedWith(Names.named("table")).toInstance("test_user");
      binder.bind(String.class).annotatedWith(Names.named("family")).toInstance("profile");
    }
  }

  @Test
  public void testSave() throws AvroBaseException {
    AvroBase<User, String, String> userHAB = AvroBaseFactory.createAvroBase(new MABModule(), MAB.class, AvroFormat.BINARY);
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
    String row = "spullara";
    userHAB.put(row, saved);
    Row<User, String> loaded = userHAB.get(row);
    assertEquals(saved, loaded.value);
    assertTrue(userHAB.put(row, loaded.value, loaded.version));
    assertFalse(userHAB.put(row, loaded.value, loaded.version));
  }

  private Utf8 $(String string) {
    return new Utf8(string);
  }
}
