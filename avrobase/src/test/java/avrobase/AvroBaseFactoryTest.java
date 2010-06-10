package avrobase;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Named;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the factory functionality
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 5:29:26 PM
 */
public class AvroBaseFactoryTest {
  public static class TestAvroBase<T extends SpecificRecord> implements AvroBase<T> {

    @Inject
    @Named("table")
    public byte[] table;

    @Inject
    @Named("family")
    public byte[] family;
    
    public boolean inited;

    @Override
    public void init() throws AvroBaseException {
      inited = true;
    }

    @Override
    public Row<T> get(byte[] row) throws AvroBaseException {
      return null;
    }

    @Override
    public void put(byte[] row, T value) throws AvroBaseException {
    }

    @Override
    public boolean put(byte[] row, T value, long version) throws AvroBaseException {
      return false;
    }

    @Override
    public Iterable<Row<T>> scan(byte[] startRow, byte[] stopRow) throws AvroBaseException {
      return null;
    }
  }

  @Test
  public void testFactory() throws AvroBaseException {
    byte[] table = "table".getBytes();
    byte[] family = "family".getBytes();
    TestAvroBase<SpecificRecord> base = (TestAvroBase<SpecificRecord>) AvroBaseFactory.createAvroBase(new Module() {
      @Override
      public void configure(Binder binder) {
      }
    }, TestAvroBase.class, table, family, AvroFormat.JSON);
    assertEquals("table", new String(base.table));
    assertEquals("family", new String(base.family));
    assertTrue(base.inited);
  }
}
