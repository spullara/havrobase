package havrobase;

import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
public class HAB<T extends SpecificRecord> {

  // HBase Config
  private HTablePool pool;
  private HBaseAdmin admin;

  // HBase Constants
  public static final byte[] VERSION_COLUMN = Bytes.toBytes("v");
  public static final byte[] SCHEMA_COLUMN = Bytes.toBytes("s");
  public static final byte[] DATA_COLUMN = Bytes.toBytes("d");
  public static final byte[] SCHEMA_TABLE = Bytes.toBytes("schema");
  public static final byte[] AVRO_FAMILY = Bytes.toBytes("avro");

  // Caching
  private Map<String, Schema> schemaCache = new ConcurrentHashMap<String, Schema>();
  private Map<Schema, String> hashCache = new ConcurrentHashMap<Schema, String>();

  // Typed return value with metadata

  static class Row<T extends SpecificRecord> {
    T value;
    byte[] row;
    long timestamp = Long.MAX_VALUE;
    long version = -1;
  }

  @Inject
  public HAB(HTablePool pool, HBaseAdmin admin) throws HAvroBaseException {
    this.pool = pool;
    this.admin = admin;
    HTable schemaTable;
    try {
      schemaTable = pool.getTable(SCHEMA_TABLE);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof TableNotFoundException) {
        HColumnDescriptor family = new HColumnDescriptor(AVRO_FAMILY);
        family.setMaxVersions(1);
        family.setCompressionType(Compression.Algorithm.LZO);
        family.setInMemory(true);
        HTableDescriptor tableDesc = new HTableDescriptor(SCHEMA_TABLE);
        tableDesc.addFamily(family);
        try {
          admin.createTable(tableDesc);
        } catch (IOException e1) {
          throw new HAvroBaseException(e1);
        }
        schemaTable = pool.getTable(SCHEMA_TABLE);
      } else {
        e.printStackTrace();
        throw new HAvroBaseException(e.getCause());
      }
    }
    try {
      Scan scan = new Scan();
      scan.addColumn(AVRO_FAMILY, SCHEMA_COLUMN);
      ResultScanner scanner = schemaTable.getScanner(scan);
      for (Result result : scanner) {
        String row = $_(result.getRow());
        byte[] value = result.getValue(SCHEMA_COLUMN);
        loadSchema(value, row);
      }
    } catch (IOException e) {
      throw new HAvroBaseException(e);
    } finally {
      pool.putTable(schemaTable);
    }
  }

  private Schema loadSchema(byte[] value, String row) throws IOException {
    Schema schema = Schema.parse(new ByteArrayInputStream(value));
    schemaCache.put(row, schema);
    hashCache.put(schema, row);
    return schema;
  }

  public Row<T> getRow(byte[] tableName, byte[] columnFamily, byte[] row) throws HAvroBaseException {
    Row<T> rowResult = new Row<T>();
    rowResult.row = row;
    HTable table = getTable(tableName, columnFamily);
    try {
      Result result = getHBaseRow(table, row, columnFamily);
      byte[] latest = getMetaData(rowResult, result, columnFamily);
      if (latest != null) {
        Schema schema = loadSchema(result, row, columnFamily);
        rowResult.value = readValue(latest, schema);
      }
    } catch (IOException e) {
      throw new HAvroBaseException(e);
    } finally {
      pool.putTable(table);
    }
    return rowResult;
  }

  public void putRow(byte[] tableName, byte[] columnFamily, byte[] row, T value) throws HAvroBaseException {
    HTable table = getTable(tableName, columnFamily);
    try {
      Schema schema = value.getSchema();
      String schemaKey;
      synchronized (schema) {
        schemaKey = hashCache.get(schema);
        if (schemaKey == null) {
          // Hash the schema, store it
          
        }
      }
    } finally {
      pool.putTable(table);
    }
  }


  private Result getHBaseRow(HTable table, byte[] row, byte[] columnFamily) throws IOException {
    Get get = new Get(row);
    get.addColumn(columnFamily, DATA_COLUMN);
    get.addColumn(columnFamily, SCHEMA_COLUMN);
    get.addColumn(columnFamily, VERSION_COLUMN);
    return table.get(get);
  }

  private T readValue(byte[] latest, Schema schema) throws IOException {
    DecoderFactory factory = new DecoderFactory();
    BinaryDecoder bd = factory.createBinaryDecoder(new ByteArrayInputStream(latest), null);
    SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema);
    return sdr.read(null, bd);
  }

  private Schema loadSchema(Result result, byte[] row, byte[] columnFamily) throws HAvroBaseException, IOException {
    byte[] schemaKey = result.getValue(columnFamily, SCHEMA_COLUMN);
    if (schemaKey == null) {
      throw new HAvroBaseException("Schema not set for row: " + $_(row));
    }
    Schema schema = schemaCache.get($_(schemaKey));
    if (schema == null) {
      HTable schemaTable = pool.getTable(SCHEMA_TABLE);
      try {
        Get schemaGet = new Get(schemaKey);
        schemaGet.addColumn(AVRO_FAMILY, SCHEMA_COLUMN);
        byte[] schemaBytes = schemaTable.get(schemaGet).getValue(AVRO_FAMILY, SCHEMA_COLUMN);
        if (schemaBytes == null) {
          throw new HAvroBaseException("No schema " + $_(schemaKey) + " found in hbase for row " + $_(row));
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

  private String $_(byte[] schemaKey) {
    return Bytes.toString(schemaKey);
  }

  private HTable getTable(byte[] tableName, byte[] columnFamily) throws HAvroBaseException {
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
          throw new HAvroBaseException(e1);
        }
        table = pool.getTable(tableName);
      } else {
        throw new HAvroBaseException(e.getCause());
      }
    }
    return table;
  }
}
