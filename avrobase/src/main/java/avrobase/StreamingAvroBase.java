package avrobase;

import org.apache.avro.specific.SpecificRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Some AvroBase implementations can directly stream their contents for export and import.
 * <p/>
 * User: sam
 * Date: 4/17/11
 * Time: 4:37 PM
 */
public interface StreamingAvroBase {
  void exportData(DataOutputStream dos);
  void exportSchema(DataOutputStream dos);
  void importData(DataInputStream dis);
  void importSchema(DataInputStream dis);
}
