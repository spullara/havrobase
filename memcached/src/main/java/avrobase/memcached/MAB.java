package avrobase.memcached;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import com.danga.MemCached.MemCachedClient;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.schooner.MemCached.MemcachedItem;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.NotImplementedException;

/**
 * Memcached implementation used for caching only. No queries.
 * <p/>
 * User: sam
 * Date: Jun 23, 2010
 * Time: 12:14:36 PM
 */
public class MAB<T extends SpecificRecord> extends AvroBaseImpl<T> {
  private String prekey;
  private String schemaPrekey;
  private MemCachedClient client;

  @Inject
  public MAB(
          MemCachedClient client,
          @Named("table") byte[] table,
          @Named("family") byte[] family,
          @Named("schema") byte[] schemaPrekey,
          AvroFormat format
  ) {
    super(format);
    this.client = client;
    prekey = new StringBuilder().append(new String(table)).append(":").append(new String(family)).append(":").toString();
    this.schemaPrekey = new StringBuilder(new String(schemaPrekey)).append(":").toString();
  }

  @Override
  public Row<T> get(byte[] row) throws AvroBaseException {
    String key = $_(row);
    MemcachedItem memcachedItem = client.gets(prekey + key);
    long version = memcachedItem.getCasUnique();
    byte[] bytes = (byte[]) memcachedItem.getValue();
    String schemaKey = $_((byte[])client.get(prekey + schemaPrekey + key));
    Schema schema = schemaCache.get(schemaKey);
    if (schema == null) {
      byte[] schemab = (byte[]) client.get(schemaPrekey + schemaKey);
      schema = loadSchema(schemab, schemaKey);
    }
    return new Row<T>(readValue(bytes, schema, format), row, 0, version) ;
  }

  @Override
  public byte[] create(T value) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public void put(byte[] row, T value) throws AvroBaseException {
    String key = $_(row);
    Schema schema = value.getSchema();
    String schemaKey = getSchemaKey(schema);
    byte[] bytes = serialize(value);
    client.set(prekey + key, bytes);
    client.set(prekey + schemaPrekey + key, schemaKey.getBytes());
  }

  @Override
  public boolean put(byte[] row, T value, long version) throws AvroBaseException {
    String key = $_(row);
    Schema schema = value.getSchema();
    String schemaKey = getSchemaKey(schema);
    byte[] bytes = serialize(value);
    boolean b = client.cas(prekey + key, bytes, version);
    if (b) {
      client.set(prekey + schemaPrekey + key, schemaKey.getBytes());
    }
    return b;
  }

  @Override
  public void delete(byte[] row) throws AvroBaseException {
    client.delete($_(row));
  }

  private String getSchemaKey(Schema schema) {
    String schemaKey = hashCache.get(schema);
    if (schemaKey == null) {
      String doc = schema.toString();
      schemaKey = createSchemaKey(schema, doc);
      client.set(schemaPrekey + schemaKey, doc.getBytes(UTF8));
    }
    return schemaKey;
  }

  @Override
  public Iterable<Row<T>> scan(byte[] startRow, byte[] stopRow) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Row<T>> search(String query, int start, int rows) throws AvroBaseException {
    throw new NotImplementedException();
  }
}
