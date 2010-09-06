package avrobase;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
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
  public static class TestAvroBase<K> implements AvroBase<SpecificRecord, byte[], String> {

    public boolean inited;
    private byte[] table;
    private byte[] family;
    private AvroFormat format;

    @Inject
    public TestAvroBase(@Named("table") byte[] table, @Named("family") byte[] family, AvroFormat format) {
      this.table = table;
      this.family = family;
      this.format = format;
      inited = true;
    }

    @Override
    public Row<SpecificRecord, byte[]> get(byte[] row) throws AvroBaseException {
      return null;
    }

    @Override
    public byte[] create(SpecificRecord value) throws AvroBaseException {
      return new byte[0];
    }

    @Override
    public void put(byte[] row, SpecificRecord value) throws AvroBaseException {
    }

    @Override
    public boolean put(byte[] row, SpecificRecord value, long version) throws AvroBaseException {
      return false;
    }

    @Override
    public void delete(byte[] row) throws AvroBaseException {
    }

    @Override
    public Iterable<Row<SpecificRecord, byte[]>> scan(byte[] startRow, byte[] stopRow) throws AvroBaseException {
      return null;
    }

    @Override
    public Iterable<Row<SpecificRecord, byte[]>> search(String query) throws AvroBaseException {
      return null;
    }

    @Override
    public Row<SpecificRecord, byte[]> mutate(byte[] row, Mutator<SpecificRecord> specificRecordMutator) throws AvroBaseException {
      return null;
    }

    @Override
    public Row<SpecificRecord, byte[]> mutate(byte[] row, Mutator<SpecificRecord> specificRecordMutator, Creator<SpecificRecord> specificRecordCreator) throws AvroBaseException {
      return null;
    }
  }

  @Test
  public void testFactory() throws AvroBaseException {
    final byte[] table = "table".getBytes();
    final byte[] family = "family".getBytes();
    TestAvroBase base = (TestAvroBase) AvroBaseFactory.createAvroBase(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(byte[].class).annotatedWith(Names.named("table")).toInstance(table);
        binder.bind(byte[].class).annotatedWith(Names.named("family")).toInstance(family);
      }
    }, TestAvroBase.class, AvroFormat.JSON);
    assertEquals("table", new String(base.table));
    assertEquals("family", new String(base.family));
    assertEquals(AvroFormat.JSON, base.format);
    assertTrue(base.inited);
  }
}
