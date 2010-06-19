package avrobase.cassandra;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.Row;
import org.apache.avro.specific.SpecificRecord;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Jun 18, 2010
 * Time: 6:59:46 PM
 */
public class CAB<T extends SpecificRecord> implements AvroBase<T> {
  @Override
  public Row<T> get(byte[] row) throws AvroBaseException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void put(byte[] row, T value) throws AvroBaseException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean put(byte[] row, T value, long version) throws AvroBaseException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Iterable<Row<T>> scan(byte[] startRow, byte[] stopRow) throws AvroBaseException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
