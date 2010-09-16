package avrobase;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class with built in serialization and deserialization and schema management.
 * <p/>
 * User: sam
 * Date: Jun 23, 2010
 * Time: 12:21:33 PM
 */
public abstract class AvroBaseImpl<T extends SpecificRecord, K> implements AvroBase<T, K> {

  protected Map<String, Schema> schemaCache = new ConcurrentHashMap<String, Schema>();
  protected Map<Schema, String> hashCache = new ConcurrentHashMap<Schema, String>();
  private Schema actualSchema;
  protected AvroFormat format;

  protected static final Charset UTF8 = Charset.forName("utf-8");

  /**
   * The serializer needs to know what format you would like new records
   * to be stored in. Old records are not affected when read as it stores their
   * format alongside them.
   *
   * @param format
   */
  public AvroBaseImpl(Schema actualSchema, AvroFormat format) {
    this.actualSchema = actualSchema;
    this.format = format;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    Row<T, K> tRow;
    do {
      // Grab the current version
      tRow = get(row);

      // If it doesn't exist, create a new one
      if (tRow == null && tCreator != null) {
        final T newValue = tCreator.create();
        if (newValue != null) {
          tRow = new Row<T,K>(newValue, row, 0);
        }
      }

      if (tRow != null) {
        T value = tMutator.mutate(tRow.value);
        // Mutator can abort the mutation
        if (value == null) return tRow;
        // Optimistically set the row
        if (put(row, value, tRow.version)) {
          return new Row<T, K>(value, row, tRow.version + 1);
        }
      } else {
        return null;
      }
      // On failure to set, try again
    } while (true);
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    return mutate(row, tMutator, null);
  }

  /**
   * Load a schema from the schema table
   */
  protected Schema loadSchema(byte[] value, String row) throws AvroBaseException {
    Schema schema;
    try {
      schema = Schema.parse(new ByteArrayInputStream(value));
    } catch (IOException e) {
      throw new AvroBaseException("Failed to deserialize schema: " + row, e);
    }
    schemaCache.put(row, schema);
    hashCache.put(schema, row);
    return schema;
  }

  /**
   * Serialize the Avro instance using its schema and the
   * format set for this avrobase
   */
  protected byte[] serialize(T value) throws AvroBaseException {
    try {
      Schema schema = value.getSchema();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Encoder be;
      switch (format) {
        case JSON:
          be = new JsonEncoder(schema, baos);
          break;
        case BINARY:
        default:
          be = new BinaryEncoder(baos);
          break;
      }
      SpecificDatumWriter<T> sdw = new SpecificDatumWriter<T>(schema);
      sdw.write(value, be);
      be.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new AvroBaseException("Failed to serialize", e);
    }
  }

  /**
   * TODO: I really need to use a lookup table to reduce the per row overhead of schema references.
   *
   * @param schema
   * @param doc
   * @return
   */
  protected String createSchemaKey(Schema schema, String doc) {
    String schemaKey;
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      md = null;
    }
    if (md == null) {
      schemaKey = doc;
    } else {
      schemaKey = new String(Hex.encodeHex(md.digest(doc.getBytes())));
    }
    schemaCache.put(schemaKey, schema);
    hashCache.put(schema, schemaKey);
    return schemaKey;
  }

  /**
   * Read the avro serialized data using the specified schema and format
   * in the hbase row
   */
  protected T
  readValue(byte[] latest, Schema schema, AvroFormat format) throws AvroBaseException {
    try {
      Decoder d;
      switch (format) {
        case JSON:
          d = new JsonDecoder(schema, new ByteArrayInputStream(latest));
          break;
        case BINARY:
        default:
          DecoderFactory factory = new DecoderFactory();
          d = factory.createBinaryDecoder(new ByteArrayInputStream(latest), null);
          break;
      }
      SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(actualSchema);
      sdr.setExpected(schema);
      return sdr.read(null, d);
    } catch (IOException e) {
      throw new AvroBaseException("Failed to read value", e);
    }
  }
}
