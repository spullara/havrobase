package avrobase;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Row value wrapper for the associated metadata.  Wouldn't Java be great if you could add metadata to any instance
 * in a typed fashion?
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 12:07:47 PM
 */
public class Row<T extends SpecificRecord, K> implements Externalizable, Cloneable {

  private static EncoderFactory encoderFactory = new EncoderFactory();
  private static DecoderFactory decoderFactory = new DecoderFactory();

  public T value;
  public K row;
  public long version;

  public Row() {}

  public Row(T value, K row) {
    this(value, row, -1);
  }

  public Row(T value, K row, long version) {
    this.value = value;
    this.row = row;
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Row row = (Row) o;
    return !(value != null ? !value.equals(row.value) : row.value != null);
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "[" + row + ", " + version + ", " + value + "]";
  }

  public void writeExternal(ObjectOutput objectOutput) throws IOException {
    // row
    objectOutput.writeObject(row);
    // version
    objectOutput.writeLong(version);
    // schema
    objectOutput.writeUTF(value.getSchema().toString());
    Schema schema = value.getSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder be = encoderFactory.binaryEncoder(baos, null);
    SpecificDatumWriter<T> sdw = new SpecificDatumWriter<T>(schema);
    sdw.write(value, be);
    be.flush();
    byte[] bytes = baos.toByteArray();
    // length
    objectOutput.writeInt(bytes.length);
    // bytes
    objectOutput.write(bytes);
  }

  public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
    // row
    row = (K) objectInput.readObject();
    // version
    version = objectInput.readLong();
    // schema
    Schema schema = Schema.parse(objectInput.readUTF());
    // length
    byte[] bytes = new byte[objectInput.readInt()];
    // bytes
    objectInput.readFully(bytes);
    Decoder d = decoderFactory.binaryDecoder(bytes, 0, bytes.length, null);
    SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema);
    value = sdr.read(null, d);
  }

  public Row<T, K> clone() {
    Schema schema = value.getSchema();
    T newvalue;
    try {
      newvalue = (T) Class.forName(schema.getFullName()).newInstance();
    } catch (Exception e) {
      throw new AvroBaseException("Could not clone row", e);
    }
    for (Schema.Field field : schema.getFields()) {
      int pos = field.pos();
      newvalue.put(pos, value.get(pos));
    }
    return new Row<T, K>(newvalue, row, version);
  }
}
