package avrobase.redis;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.TimeoutException;

/**
 * AvroBase on top of Redis
 * <p/>
 * User: sam
 * Date: Oct 3, 2010
 * Time: 11:32:10 AM
 */
public class RAB<T extends SpecificRecord> extends AvroBaseImpl<T, String> {

  private JedisPool pool;
  private int db;

  private static final String d = "_d";
  private static final String s = "_s";
  private static final String v = "_v";
  private static final String z = "_z";

  public RAB(JedisPool pool, int db, Schema actualSchema) {
    super(actualSchema, AvroFormat.JSON);
    this.pool = pool;
    this.db = db;
  }

  @Override
  public Row<T, String> get(String row) throws AvroBaseException {
    try {
      boolean returned = false;
      Jedis j = pool.getResource();
      try {
        j.select(db);
        String schemaId = j.get(row + s);
        String schemaStr = j.get(schemaId + z);
        Schema schema = Schema.parse(schemaStr);
        long version = Long.parseLong(j.get(row + v));
        return new Row<T, String>(readValue(j.get(row + d).getBytes(), schema, format), row, version);
      } catch (Exception e) {
        pool.returnBrokenResource(j);
        returned = true;
        throw new AvroBaseException(e);
      } finally {
        if (!returned) pool.returnResource(j);
      }
    } catch (TimeoutException e) {
      throw new AvroBaseException("Timed out", e);
    }
  }

  @Override
  public String create(T value) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public void put(String row, T value) throws AvroBaseException {
    try {
      boolean returned = false;
      Jedis j = pool.getResource();
      try {
        j.select(db);
        String versionStr = j.get(row + v);
        long version = versionStr == null ? 0 : Long.parseLong(versionStr) + 1;
        Schema schema = value.getSchema();
        String doc = schema.toString();
        String schemaKey = createSchemaKey(schema, doc);
        j.set(row + s, schemaKey);
        j.set(schemaKey + z, doc);
        j.set(row + v, String.valueOf(version));
        j.set(row + d, new String(serialize(value), UTF8));
      } catch (Exception e) {
        pool.returnBrokenResource(j);
        returned = true;
        throw new AvroBaseException(e);
      } finally {
        if (!returned) pool.returnResource(j);
      }
    } catch (TimeoutException e) {
      throw new AvroBaseException("Timed out", e);
    }
  }

  @Override
  public boolean put(String row, T value, long version) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public void delete(String row) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Row<T, String>> scan(String startRow, String stopRow) throws AvroBaseException {
    throw new NotImplementedException();
  }
}
