package avrobase.gae;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.Creator;
import avrobase.Mutator;
import avrobase.Row;
import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation on top of Google App Engine datastore. Supports only Long and String for keys.
 * <p/>
 * User: sam
 * Date: 5/11/11
 * Time: 1:40 PM
 */
public class GAEAB<T extends SpecificRecord, K> implements AvroBase<T, K> {
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private final Schema readerSchema;
  private final String entityName;
  private final String schemaEntityName;
  private DatastoreService ds;
  private AsyncDatastoreService ads;
  private Class<?> aClass;

  /**
   * The serializer needs to know what format you would like new records
   * to be stored in. Old records are not affected when read as it stores their
   * format alongside them.
   *
   */
  public GAEAB(Schema readerSchema, String schemaEntityName) {
    this.readerSchema = readerSchema;
    this.schemaEntityName = schemaEntityName;
    entityName = readerSchema.getFullName();
    ds = DatastoreServiceFactory.getDatastoreService();
    ads = DatastoreServiceFactory.getAsyncDatastoreService();
    try {
      aClass = Class.forName(readerSchema.getFullName());
    } catch (ClassNotFoundException e) {
      throw new AvroBaseException("Could not find schema class", e);
    }
    try {
      T sr = (T) aClass.newInstance();
    } catch (Exception e) {
      throw new AvroBaseException("Could not instantiate schema type", e);
    }
  }

  private Map<Long, Schema> schemas = new ConcurrentHashMap<Long, Schema>();
  private Map<Schema, Long> reverseSchema = new ConcurrentHashMap<Schema, Long>();

  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    Key key = getKey(row);
    try {
      Entity entity = ds.get(key);
      Map<String, Object> properties = entity.getProperties();
      long schemaId = (Long) properties.get("avrobase.schema");
      Schema writerSchema = schemas.get(schemaId);
      if (writerSchema == null) {
        Entity schemaEntity = ds.get(KeyFactory.createKey(schemaEntityName, schemaId));
        if (schemaEntity == null) {
          throw new AvroBaseException("Failed to find schema: " + schemaId);
        }
        writerSchema = Schema.parse((String) schemaEntity.getProperty("schema"));
      }
      Schema schema = Schema.applyAliases(writerSchema, readerSchema);
      return new Row<T, K>((T) applyFields(entity, schema), row);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private Key getKey(K row) {
    Key key;
    if (row instanceof String) {
      key = KeyFactory.createKey(entityName, (String) row);
    } else if (row instanceof Long) {
      key = KeyFactory.createKey(entityName, (Long) row);
    } else {
      throw new AvroBaseException("Invalid key type: " + row.getClass().getName());
    }
    return key;
  }

  private SpecificRecord applyFields(Entity entity, Schema schema) {
    SpecificRecord sr;
    try {
      sr = (SpecificRecord) aClass.newInstance();
    } catch (Exception e) {
      throw new AvroBaseException("Failed to instantiate", e);
    }
    if (schema.getType() != Schema.Type.RECORD) {
      throw new AvroBaseException("Schema is not a record");
    }
    for (Schema.Field field : schema.getFields()) {
      sr.put(field.pos(), getValue(field.schema(), entity.getProperty(field.name())));
    }
    return sr;
  }

  private Object getValue(Schema schema, Object value) {
    Schema.Type type = schema.getType();
    Object result = null;
    if (value == null) {
      // TODO: default values
      result = null;
    } else {
      if (value instanceof Text) {
        value = ((Text)value).getValue();
      }
      switch (type) {
        case ARRAY: {
          Schema elementType = schema.getElementType();
          GenericArray ga = new GenericData.Array(0, elementType);
          Collection list = (Collection) value;
          for (Object o : list) {
            ga.add(getValue(elementType, o));
          }
          result = ga;
          break;
        }
        case MAP: {
          Schema valueType = schema.getValueType();
          Map avroMap = new HashMap();
          Map<Object, Object> map = (Map<Object, Object>) value;
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            avroMap.put(getValue(STRING_SCHEMA, entry.getKey()), getValue(valueType, entry.getValue()));
          }
          result = avroMap;
          break;
        }
        case UNION: {
          for (Schema unionType : schema.getTypes()) {
            result = getValue(unionType, value);
            if (result != null) break;
          }
          break;
        }
        case RECORD: {
          Key subkey = (Key) value;
          try {
            Entity child = ds.get(subkey);
            result = applyFields(child, schema);
          } catch (EntityNotFoundException e) {
            result = null;
          }
          break;
        }
        case BOOLEAN:
        case DOUBLE:
        case FIXED:
        case FLOAT:
        case INT:
        case LONG:
        case NULL:
        case STRING: {
          result = value;
          break;
        }
        case BYTES: {
          if (value instanceof Blob) {
            result = ((Blob)value).getBytes();
          } else if (value instanceof ShortBlob) {
            result = ((ShortBlob)value).getBytes();
          }
          break;
        }
        case ENUM: {
          result = schema.getEnumOrdinal((String) value);
          break;
        }
      }
    }
    return result;
  }

  @Override
  public K create(T value) throws AvroBaseException {
    return null;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    Long schemaId = reverseSchema.get(readerSchema);
    if (schemaId == null) {
      Key put = ds.put(new Entity(schemaEntityName));
      long id = put.getId();
      schemas.put(id, readerSchema);
      reverseSchema.put(readerSchema, id);
    }
    Key key = getKey(row);
    Entity entity = applyFields(key, value, readerSchema);

  }

  private Entity applyFields(Key key, SpecificRecord value, Schema schema) {
    Entity entity = new Entity(schema.getFullName(), key);
    for (Schema.Field field : schema.getFields()) {
      entity.setProperty(field.name(), getProperty(key, field.schema(), value.get(field.pos())));
    }
    return entity;
  }

  private Entity applyFields(SpecificRecord value, Schema schema, Key parent) {
    Entity entity = new Entity(schema.getFullName(), parent);
    for (Schema.Field field : schema.getFields()) {
      entity.setProperty(field.name(), getProperty(entity.getKey(), field.schema(), value.get(field.pos())));
    }
    return entity;
  }

  private Object getProperty(Key parent, Schema schema, Object value) {
    Schema.Type type = schema.getType();
    Object result = null;
    if (value == null) {
      // TODO: default values
      result = null;
    } else {
      switch (type) {
        case ARRAY: {
          List entityList = new ArrayList();
          List list = (List) value;
          for (Object o : list) {
            entityList.add(getProperty(parent, schema.getElementType(), o));
          }
          result = entityList;
          break;
        }
        case MAP: {
          Map<CharSequence, Object> map = (Map<CharSequence, Object>) value;
          Map<String, Object> entityMap = new HashMap<String, Object>();
          for (Map.Entry<CharSequence, Object> entry : map.entrySet()){
            entityMap.put(entry.getKey().toString(), getProperty(parent, schema.getValueType(), entry.getValue()));
          }
          result = entityMap;
          break;
        }
        case UNION: {
          for (Schema schema1 : schema.getTypes()) {
            result = getProperty(parent, schema1, value);
            if (result != null) break;
          }
          break;
        }
        case RECORD: {
          result = applyFields((SpecificRecord) value, schema, parent);
          break;
        }
        case BOOLEAN:
        case DOUBLE:
        case FIXED:
        case FLOAT:
        case INT:
        case LONG:
        case NULL:
        case STRING: {
          result = value;
        }
        case BYTES: {
          if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            if (bytes.length > 500) {
              result = new ShortBlob(bytes);
            } else result = new Blob(bytes);
          }
          break;
        }
        case ENUM: {
          result = value.toString();
          break;
        }
      }
    }
    return result;
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    return false;
  }

  @Override
  public void delete(K row) throws AvroBaseException {
  }

  @Override
  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    return null;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    return null;
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    return null;
  }
}
