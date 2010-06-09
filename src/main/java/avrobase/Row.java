package avrobase;

import org.apache.avro.specific.SpecificRecord;

/**
* TODO: Edit this
* <p/>
* User: sam
* Date: Jun 9, 2010
* Time: 12:07:47 PM
*/
public class Row<T extends SpecificRecord> {
  public T value;
  public byte[] row;
  public long timestamp = Long.MAX_VALUE;
  public long version = -1;
}
