package avrobase.redis;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import com.google.common.base.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisException;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
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
  private Supplier<String> kg;

  private static final String d = "_d";
  private static final String s = "_s";
  private static final String v = "_v";
  private static final String z = "_z";

  public RAB(JedisPool pool, int db,  Supplier<String> kg, Schema actualSchema) {
    super(actualSchema, AvroFormat.JSON);
    this.pool = pool;
    this.db = db;
    this.kg = kg;
  }

  @Override
  public Row<T, String> get(final String row) throws AvroBaseException {
    try {
      boolean returned = false;
      final Jedis j = pool.getResource();
      try {
        j.select(db);
        List<Object> results;
        do {
          results = j.multi(new TransactionBlock() {
            @Override
            public void execute() throws JedisException {
              get(row + s);
              get(row + v);
              get(row + d);
            }
          });
        } while (results == null);
        if (results.size() != 3) {
          throw new AvroBaseException("Incorrect number of results from redis transaction: " + results);
        }
        String schemaId = (String) results.get(0);
        String versionStr = (String) results.get(1);
        String data = (String) results.get(2);
        if (data == null) {
          return null;
        }
        Schema schema = schemaCache.get(schemaId);
        if (schema == null) {
          schema = loadSchema(j.get(schemaId + z).getBytes(), schemaId);
        }
        return new Row<T, String>(readValue(data.getBytes(), schema, format), row, Long.parseLong(versionStr));
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
    String row = kg.get();
    put(row, value);
    return row;
  }

  @Override
  public void put(final String row, final T value) throws AvroBaseException {
    try {
      boolean returned = false;
      Jedis j = pool.getResource();
      try {
        j.select(db);
        Schema schema = value.getSchema();
        String schemaKey = hashCache.get(schema);
        if (schemaKey == null) {
          final String doc = schema.toString();
          schemaKey = createSchemaKey(schema, doc);
          j.set(schemaKey + z, doc);
        }
        final String finalSchemaKey = schemaKey;
        List<Object> results;
        do {
          results = j.multi(new TransactionBlock() {
            @Override
            public void execute() throws JedisException {
              incr(row + v);
              set(row + s, finalSchemaKey);
              set(row + d, new String(serialize(value), UTF8));
            }
          });
        } while (results == null);
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
  public boolean put(final String row, final T value, final long version) throws AvroBaseException {
    try {
      boolean returned = false;
      final Jedis j = pool.getResource();
      try {
        j.select(db);
        Schema schema = value.getSchema();
        String schemaKey = hashCache.get(schema);
        if (schemaKey == null) {
          final String doc = schema.toString();
          schemaKey = createSchemaKey(schema, doc);
          j.set(schemaKey + z, doc);
        }
        String watch = j.watch(row + v);
        if (!watch.equals("OK")) {
          return false;
        }
        String v = j.get(row + RAB.v);
        if (!v.equals(String.valueOf(version))) {
          return false;
        }
        final String finalSchemaKey = schemaKey;
        List<Object> results = j.multi(new TransactionBlock() {
          @Override
          public void execute() throws JedisException {
            incr(row + RAB.v);
            set(row + s, finalSchemaKey);
            set(row + d, new String(serialize(value), UTF8));
          }
        });
        return results != null;
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
  public void delete(final String row) throws AvroBaseException {
    try {
      boolean returned = false;
      Jedis j = pool.getResource();
      try {
        j.select(db);
        List<Object> results;
        do {
          results = j.multi(new TransactionBlock() {
            @Override
            public void execute() throws JedisException {
              del(row + v);
              del(row + d);
              del(row + s);
            }
          });
        } while (results == null);
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
  public Iterable<Row<T, String>> scan(String startRow, String stopRow) throws AvroBaseException {
    throw new NotImplementedException();
  }
}
