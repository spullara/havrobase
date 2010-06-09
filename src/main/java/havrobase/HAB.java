package havrobase;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.AvroFormat;
import avrobase.Row;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HAvroBase client.
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:13:35 PM
 */
public class HAB<T extends SpecificRecord> implements AvroBase<T> {

  // HBase Config
  @Inject
  private HTablePool pool;
  @Inject
  private HBaseAdmin admin;

  // HBase Constants
  public static final byte[] VERSION_COLUMN = $("v");
  public static final byte[] SCHEMA_COLUMN = $("s");
  public static final byte[] DATA_COLUMN = $("d");
  public static final byte[] AVRO_FAMILY = $("avro");
  public static final byte[] FORMAT_COLUMN = $("f");

  // Format
  @Inject(optional = true)
  private AvroFormat format = AvroFormat.BINARY;

  // Table
  @Inject
  @Named("table")
  private byte[] tableName;

  // Family
  @Inject
  @Named("family")
  private byte[] family;

  // Schema
  @Inject(optional = true)
  @Named("schema")
  private byte[] schemaTable = $("schema");

  // Caching
  private Map<String, Schema> schemaCache = new ConcurrentHashMap<String, Schema>();
  private Map<Schema, String> hashCache = new ConcurrentHashMap<Schema, String>();

  // Typed return value with metadata  

  /**
   * Load the schema map on init and then keep it up to date from then on. The HBase
   * connectivity is provided usually via Guice.
   *
   * @param pool
   * @param admin
   * @throws AvroBaseException
   */
  @Inject
  public void init() throws AvroBaseException {
    HTable schemaTable;
    try {
      schemaTable = pool.getTable(this.schemaTable);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof TableNotFoundException) {
        HColumnDescriptor family = new HColumnDescriptor(AVRO_FAMILY);
        family.setMaxVersions(1);
        family.setCompressionType(Compression.Algorithm.LZO);
        family.setInMemory(true);
        HTableDescriptor tableDesc = new HTableDescriptor(this.schemaTable);
        tableDesc.addFamily(family);
        try {
          admin.createTable(tableDesc);
        } catch (IOException e1) {
          throw new AvroBaseException(e1);
        }
        schemaTable = pool.getTable(this.schemaTable);
      } else {
        e.printStackTrace();
        throw new AvroBaseException(e.getCause());
      }
    }
    try {
      Scan scan = new Scan();
      scan.addColumn(AVRO_FAMILY, SCHEMA_COLUMN);
      ResultScanner scanner = schemaTable.getScanner(scan);
      for (Result result : scanner) {
        String row = $_(result.getRow());
        byte[] value = result.getValue(AVRO_FAMILY, SCHEMA_COLUMN);
        loadSchema(value, row);
      }
    } catch (IOException e) {
      throw new AvroBaseException(e);
    } finally {
      pool.putTable(schemaTable);
    }
  }

  // Load a schema from the schema table

  private Schema loadSchema(byte[] value, String row) throws IOException {
    Schema schema = Schema.parse(new ByteArrayInputStream(value));
    schemaCache.put(row, schema);
    hashCache.put(schema, row);
    return schema;
  }

  @Override
  public Row<T> get(byte[] row) throws AvroBaseException {
    Row<T> rowResult = new Row<T>();
    rowResult.row = row;
    HTable table = getTable(tableName, family);
    try {
      Result result = getHBaseRow(table, row, family);
      populateRowResult(rowResult, result, row);
    } catch (IOException e) {
      throw new AvroBaseException(e);
    } finally {
      pool.putTable(table);
    }
    return rowResult;
  }

  private void populateRowResult(Row<T> rowResult, Result result, byte[] row) throws AvroBaseException {
    try {
      byte[] latest = getMetaData(rowResult, result, family);
      if (latest != null) {
        Schema schema = loadSchema(result, row, family);
        byte[] formatB = result.getValue(family, FORMAT_COLUMN);
        AvroFormat format = AvroFormat.BINARY;
        if (formatB != null) {
          format = AvroFormat.values()[Bytes.toInt(formatB)];
        }
        rowResult.value = readValue(latest, schema, format);
      }
    } catch (IOException e) {
      throw new AvroBaseException(e);
    }
  }

  @Override
  public void put(byte[] row, T value) throws AvroBaseException {
    HTable table = getTable(tableName, family);
    long version;
    try {
      do {
        // Spin until success, last one wins.
        version = getVersion(family, row, table);
      } while (!put(row, value, version));
    } catch (IOException e) {
      throw new AvroBaseException("Failed to retrieve version for row: " + $_(row), e);
    } finally {
      pool.putTable(table);
    }
  }

  @Override
  public boolean put(byte[] row, T value, long version) throws AvroBaseException {
    HTable table = getTable(tableName, family);
    try {
      Schema schema = value.getSchema();
      String schemaKey = storeSchema(schema);
      byte[] bytes = serialize(value, schema, format);
      Put put = new Put(row);
      put.add(family, SCHEMA_COLUMN, $(schemaKey));
      put.add(family, DATA_COLUMN, bytes);
      put.add(family, VERSION_COLUMN, Bytes.toBytes(version + 1));
      put.add(family, FORMAT_COLUMN, Bytes.toBytes(format.ordinal()));
      if (version == 0) {
        table.put(put);
        return true;
      }
      return table.checkAndPut(row, family, VERSION_COLUMN, Bytes.toBytes(version), put);
    } catch (IOException e) {
      throw new AvroBaseException("Could not encode " + value, e);
    } finally {
      pool.putTable(table);
    }
  }

  @Override
  public Iterable<Row<T>> scan(byte[] startRow, byte[] stopRow) throws AvroBaseException {
    Scan scan = new Scan();
    scan.addFamily(family);
    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }
    HTable table = pool.getTable(tableName);
    try {
      ResultScanner scanner = table.getScanner(scan);
      final Iterator<Result> results = scanner.iterator();
      return new Iterable<Row<T>>() {
        @Override
        public Iterator<Row<T>> iterator() {
          return new Iterator<Row<T>>() {
            @Override
            public boolean hasNext() {
              return results.hasNext();
            }
            @Override
            public Row<T> next() {
              Result result = results.next();
              Row<T> rowResult = new Row<T>();
              byte[] row = result.getRow();
              rowResult.row = row;
              try {
                populateRowResult(rowResult, result, row);
                return rowResult;
              } catch (AvroBaseException e) {
                throw new RuntimeException(e);
              }
            }
            @Override
            public void remove() {
              throw new NotImplementedException();
            }
          };
        }
      };
    } catch (IOException e) {
      throw new AvroBaseException(e);
    } finally {
      // Is this safe?
      pool.putTable(table);
    }
  }

  private byte[] serialize(T value, Schema schema, AvroFormat format) throws IOException {
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
  }

  private long getVersion(byte[] columnFamily, byte[] row, HTable table) throws IOException {
    Get get = new Get(row);
    get.addColumn(columnFamily, VERSION_COLUMN);
    Result result = table.get(get);
    byte[] versionB = result.getValue(columnFamily, VERSION_COLUMN);
    long version;
    if (versionB == null) {
      version = 0;
    } else {
      version = Bytes.toLong(versionB);
    }
    return version;
  }

  private String storeSchema(Schema schema) throws AvroBaseException {
    String schemaKey;
    synchronized (schema) {
      schemaKey = hashCache.get(schema);
      if (schemaKey == null) {
        // Hash the schema, store it
        MessageDigest md;
        try {
          md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
          md = null;
        }
        String doc = schema.toString();
        if (md == null) {
          schemaKey = doc;
        } else {
          schemaKey = new String(Hex.encodeHex(md.digest(doc.getBytes())));
        }
        schemaCache.put(schemaKey, schema);
        hashCache.put(schema, schemaKey);
        Put put = new Put($(schemaKey));
        put.add(AVRO_FAMILY, SCHEMA_COLUMN, $(doc));
        HTable schemaTable = pool.getTable(this.schemaTable);
        try {
          schemaTable.put(put);
        } catch (IOException e) {
          throw new AvroBaseException("Could not store schema " + doc, e);
        } finally {
          pool.putTable(schemaTable);
        }
      }
    }
    return schemaKey;
  }


  private Result getHBaseRow(HTable table, byte[] row, byte[] columnFamily) throws IOException {
    Get get = new Get(row);
    get.addColumn(columnFamily, DATA_COLUMN);
    get.addColumn(columnFamily, SCHEMA_COLUMN);
    get.addColumn(columnFamily, VERSION_COLUMN);
    get.addColumn(columnFamily, FORMAT_COLUMN);
    return table.get(get);
  }

  private T readValue(byte[] latest, Schema schema, AvroFormat format) throws IOException {
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
    SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema);
    return sdr.read(null, d);
  }

  private Schema loadSchema(Result result, byte[] row, byte[] columnFamily) throws AvroBaseException, IOException {
    byte[] schemaKey = result.getValue(columnFamily, SCHEMA_COLUMN);
    if (schemaKey == null) {
      throw new AvroBaseException("Schema not set for row: " + $_(row));
    }
    Schema schema = schemaCache.get($_(schemaKey));
    if (schema == null) {
      HTable schemaTable = pool.getTable(this.schemaTable);
      try {
        Get schemaGet = new Get(schemaKey);
        schemaGet.addColumn(AVRO_FAMILY, SCHEMA_COLUMN);
        byte[] schemaBytes = schemaTable.get(schemaGet).getValue(AVRO_FAMILY, SCHEMA_COLUMN);
        if (schemaBytes == null) {
          throw new AvroBaseException("No schema " + $_(schemaKey) + " found in hbase for row " + $_(row));
        }
        schema = loadSchema(schemaBytes, $_(schemaKey));
      } finally {
        pool.putTable(schemaTable);
      }
    }
    return schema;
  }

  private byte[] getMetaData(Row<T> rowResult, Result result, byte[] columnFamily) {
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
    if (map == null) return null;
    NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = map.get(columnFamily);
    NavigableMap<Long, byte[]> values = familyData.get(DATA_COLUMN);
    long timestamp = Long.MAX_VALUE;
    byte[] latest = null;
    if (values != null) {
      for (Map.Entry<Long, byte[]> e : values.entrySet()) {
        if (e.getKey() < timestamp) {
          latest = e.getValue();
        }
      }
    }
    rowResult.timestamp = timestamp;
    byte[] version = result.getValue(columnFamily, VERSION_COLUMN);
    rowResult.version = version == null ? -1 : Bytes.toLong(version);
    return latest;
  }

  private static byte[] $(String s) {
    return Bytes.toBytes(s);
  }

  private static String $_(byte[] s) {
    return Bytes.toString(s);
  }

  private HTable getTable(byte[] tableName, byte[] columnFamily) throws AvroBaseException {
    HTable table;
    try {
      table = pool.getTable(tableName);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof TableNotFoundException) {
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        family.setMaxVersions(1);
        family.setCompressionType(Compression.Algorithm.LZO);
        family.setInMemory(false);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.addFamily(family);
        try {
          admin.createTable(tableDesc);
        } catch (IOException e1) {
          throw new AvroBaseException(e1);
        }
        table = pool.getTable(tableName);
      } else {
        throw new AvroBaseException(e.getCause());
      }
    }
    return table;
  }
}
