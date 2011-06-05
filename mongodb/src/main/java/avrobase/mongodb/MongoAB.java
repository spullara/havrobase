package avrobase.mongodb;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 6/4/11
 * Time: 6:54 PM
 */
public class MongoAB<T extends SpecificRecord, K> extends AvroBaseImpl<T, K> {

  private final Schema readerSchema;
  private final DBCollection rows;

  public MongoAB(DB db, String typeName, Schema readerSchema) {
    super(readerSchema, AvroFormat.JSON);
    this.readerSchema = readerSchema;
    this.rows = db.getCollection(typeName);
    rows.createIndex(b("id", 1));
  }

  private BasicDBObject b() {
    return new BasicDBObject();
  }

  private BasicDBObject b(String name, Object value) {
    return new BasicDBObject(name, value);
  }

  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    DBCursor cursor = rows.find(r(row));
    if (cursor.hasNext()) {
      DBObject ro = cursor.next();
      return newrow(row, ro);
    }
    return null;
  }

  private Row<T, K> newrow(K row, DBObject ro) {
    T ao = getAvroObject(ro);
    return new Row<T, K>(ao, row, (Long) ro.get("version"));
  }

  private T getAvroObject(DBObject ro) {
    BasicDBObject vo = (BasicDBObject) ro.get("value");
    Class c = SpecificData.get().getClass(readerSchema);
    T ao;
    try {
      ao = (T) c.newInstance();
      for (Schema.Field field : readerSchema.getFields()) {
        String name = field.name();
        Object v = vo.get(name);
        if (v instanceof byte[]) {
          v = ByteBuffer.wrap((byte[]) v);
        }
        ao.put(field.pos(), v);
      }
    } catch (Exception e) {
      throw new AvroBaseException("Could not create object", e);
    }
    return ao;
  }

  @Override
  public K create(T value) throws AvroBaseException {
    return null;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    BasicDBObject ro = r(row);
    ro.put("version", 1L);
    ro.put("value", getDBObject(value));
    rows.update(r(row), ro, true, false);
  }

  private BasicDBObject getDBObject(T value) {
    BasicDBObject vo = new BasicDBObject();
    for (Schema.Field field : actualSchema.getFields()) {
      int pos = field.pos();
      Object val = value.get(pos);
      if (val instanceof Utf8) {
        val = val.toString();
      } else if (val instanceof ByteBuffer) {
        val = ((ByteBuffer)val).array();
      }
      vo.put(field.name(), val);
    }
    return vo;
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    BasicDBObject ro = b("$set", r(row));
    ro.put("$set", b("value", getDBObject(value)));
    ro.put("$inc", b("version", 1));
    BasicDBObject query = r(row);
    query.put("version", version);
    return rows.update(query, ro, false, false).getN() == 1;
  }

  @Override
  public void delete(K row) throws AvroBaseException {
    rows.findAndRemove(r(row));
  }

  private BasicDBObject r(K row) {
    return new BasicDBObject("id", row);
  }

  @Override
  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    BasicDBObject b = b();
    if (startRow != null) {
      if (stopRow == null) {
        b.put("id", b("$gte", startRow));
      } else {
        BasicDBObject query = b("$gte", startRow);
        query.put("$lt", stopRow);
        b = b("id", query);
      }
    } else if (stopRow != null) b.put("id", b("$lt", stopRow));
    final DBCursor dbCursor = rows.find(b);
    dbCursor.sort(b("id", 1));
    return new Iterable<Row<T, K>>() {
      @Override
      public Iterator<Row<T, K>> iterator() {
        final Iterator<DBObject> iterator = dbCursor.iterator();
        return new Iterator<Row<T, K>>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Row<T, K> next() {
            DBObject next = iterator.next();
            return newrow((K) next.get("id"), next);
          }

          @Override
          public void remove() {
          }
        };
      }
    };
  }
}
