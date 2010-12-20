package avrobase.hs;

import avrobase.AvroBase;
import avrobase.AvroFormat;
import avrobase.Row;
import avrobase.handlersocket.HSAB;
import avrobase.mysql.BytesKeyStrategy;
import bagcheck.User;
import com.google.code.hs4j.HSClientBuilder;
import com.google.code.hs4j.impl.HSClientBuilderImpl;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.concurrent.Executors;

import static junit.framework.Assert.assertEquals;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 12/20/10
 * Time: 12:32 PM
 */
public class HSABTest {
  @Test
  public void putGet() throws IOException, SQLException {
    AvroBase<User, byte[]> userHSAB = getHSAB();
    User user = getUser();
    userHSAB.put($("test"), user);
    Row<User, byte[]> test = userHSAB.get($("test"));
    assertEquals(user, test.value);
  }

  private User getUser() {
    User user = new User();
    user.email = u("spullara@yahoo.com");
    user.firstName = u("Sam");
    user.lastName = u("Pullara");
    user.image = u("");
    user.password = ByteBuffer.allocate(0);
    return user;
  }

  private Utf8 u(String s) {
    return new Utf8(s);
  }

  byte[] $(String s) {
    return s.getBytes();
  }

  private HSAB<User, byte[]> getHSAB() throws IOException, SQLException {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Could not find JDBC driver: " + e);
    }

    final BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl("jdbc:mysql://localhost:3406/hstest");
    config.setMaxConnectionsPerPartition(50);
    config.setPartitionCount(4);
    config.setLazyInit(true);
    config.setUsername("test");
    config.setPassword("");
    BoneCPDataSource ds = new BoneCPDataSource(config);

    HSClientBuilder hsb = new HSClientBuilderImpl();
    hsb.setServerAddress("localhost", 9998);
    hsb.setConnectionPoolSize(100);

    return new HSAB<User, byte[]>(
        Executors.newCachedThreadPool(),
        ds,
        hsb.build(),
        "user",
        "profile",
        "avro_schemas",
        User.SCHEMA$,
        AvroFormat.JSON,
        new BytesKeyStrategy(new SecureRandom(), 128), "hstest"
    );
  }

}
