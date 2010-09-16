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
public class MAB<T extends SpecificRecord> extends AvroBaseImpl<T, String, String> {
  private String prekey;
  private String schemaPrekey;
  private MemCachedClient client;

  @Inject
  public MAB(
      Schema expectedFormat,
      MemCachedClient client,
      @Named("table") String table,
      @Named("family") String family,
      @Named("schema") String schemaPrekey,
      AvroFormat format
  ) {
    super(expectedFormat, format);
    this.client = client;
    prekey = new StringBuilder().append(table).append(":").append(family).append(":").toString();
    this.schemaPrekey = new StringBuilder(schemaPrekey).append(":").toString();
  }

  @Override
  public Row<T, String> get(String row) throws AvroBaseException {
    MemcachedItem memcachedItem = client.gets(prekey + row);
    long version = memcachedItem.getCasUnique();
    byte[] bytes = (byte[]) memcachedItem.getValue();
    String schemaKey = new String((byte[]) client.get(prekey + schemaPrekey + row));
    Schema schema = schemaCache.get(schemaKey);
    if (schema == null) {
      byte[] schemab = (byte[]) client.get(schemaPrekey + schemaKey);
      schema = loadSchema(schemab, schemaKey);
    }
    return new Row<T, String>(readValue(bytes, schema, format), row, version);
  }

  @Override
  public String create(T value) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public void put(String row, T value) throws AvroBaseException {
    Schema schema = value.getSchema();
    String schemaKey = getSchemaKey(schema);
    byte[] bytes = serialize(value);
    client.set(prekey + row, bytes);
    client.set(prekey + schemaPrekey + row, schemaKey.getBytes());
  }

  @Override
  public boolean put(String row, T value, long version) throws AvroBaseException {
    Schema schema = value.getSchema();
    String schemaKey = getSchemaKey(schema);
    byte[] bytes = serialize(value);
    boolean b = client.cas(prekey + row, bytes, version);
    if (b) {
      client.set(prekey + schemaPrekey + row, schemaKey.getBytes());
    }
    return b;
  }

  @Override
  public void delete(String row) throws AvroBaseException {
    client.delete(row);
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
  public Iterable<Row<T, String>> scan(String startRow, String stopRow) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Row<T, String>> search(String query) throws AvroBaseException {
    throw new NotImplementedException();
  }
}