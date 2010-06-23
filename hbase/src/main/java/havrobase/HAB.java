package havrobase;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * HAvroBase client.
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:13:35 PM
 */
public class HAB<T extends SpecificRecord> extends AvroBaseImpl<T> {

  // HBase Constants
  public static final byte[] VERSION_COLUMN = $("v");
  public static final byte[] SCHEMA_COLUMN = $("s");
  public static final byte[] DATA_COLUMN = $("d");
  public static final byte[] AVRO_FAMILY = $("avro");
  public static final byte[] FORMAT_COLUMN = $("f");

  // Cache the schemas with a two-way lookup
  private HTablePool pool;
  private HBaseAdmin admin;
  private byte[] tableName;
  private byte[] family;
  private byte[] schemaName;

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
  public HAB(
          HTablePool pool,
          HBaseAdmin admin,
          @Named("table") byte[] tableName,
          @Named("family") byte[] family,
          @Named("schema") byte[] schemaName,
          AvroFormat format
  ) throws AvroBaseException {
    super(format);
    this.pool = pool;
    this.admin = admin;
    this.tableName = tableName;
    this.family = family;
    this.schemaName = schemaName;
    HTable schemaTable;
    try {
      schemaTable = pool.getTable(this.schemaName);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof TableNotFoundException) {
        schemaTable = createSchemaTable();
      } else {
        throw new AvroBaseException(e.getCause());
      }
    }
    try {
      loadSchemas(schemaTable);
    } catch (IOException e) {
      throw new AvroBaseException(e);
    } finally {
      pool.putTable(schemaTable);
    }
    HTable table = getTable(tableName, family);
    try {
      if (table.getTableDescriptor().getFamily(family) == null) {
        HColumnDescriptor familyDesc = getColumnDesc(family);
        try {
          admin.disableTable(tableName);
          admin.addColumn(tableName, familyDesc);
          admin.enableTable(tableName);
        } catch (IOException e) {
          throw new AvroBaseException(e);
        }
      }
    } catch (IOException e) {
      throw new AvroBaseException(e);
    } finally {
      pool.putTable(table);
    }
  }

  // Load all the schemas currently registered in hbase

  private void loadSchemas(HTable schemaTable) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(AVRO_FAMILY, SCHEMA_COLUMN);
    ResultScanner scanner = schemaTable.getScanner(scan);
    for (Result result : scanner) {
      String row = $_(result.getRow());
      byte[] value = result.getValue(AVRO_FAMILY, SCHEMA_COLUMN);
      loadSchema(value, row);
    }
  }

  // Given a table and family, create a corresponding table and
  // family with hbase.

  private HTable createSchemaTable() throws AvroBaseException {
    HTable schemaTable;
    HColumnDescriptor family = new HColumnDescriptor(AVRO_FAMILY);
    family.setMaxVersions(1);
    family.setCompressionType(Compression.Algorithm.LZO);
    family.setInMemory(true);
    HTableDescriptor tableDesc = new HTableDescriptor(schemaName);
    tableDesc.addFamily(family);
    try {
      admin.createTable(tableDesc);
    } catch (IOException e1) {
      throw new AvroBaseException(e1);
    }
    schemaTable = pool.getTable(schemaName);
    return schemaTable;
  }

  @Override
  public Row<T> get(byte[] row) throws AvroBaseException {
    HTable table = getTable(tableName, family);
    try {
      Result result = getHBaseRow(table, row, family);
      return getRowResult(result, row);
    } catch (IOException e) {
      throw new AvroBaseException(e);
    } finally {
      pool.putTable(table);
    }
  }

  @Override
  public void put(byte[] row, T value) throws AvroBaseException {
    HTable table = getTable(tableName, family);
    long version;
    try {
      do {
        // FIXME: Spin until success, last one wins. Provably dangerous?
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
      byte[] bytes = serialize(value);
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
              try {
                return getRowResult(result, result.getRow());
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
      // FIXME: Is this safe?
      pool.putTable(table);
    }
  }

  // Given an HBase row result take it apart and populate the Row wrapper metadata.

  private Row<T> getRowResult(Result result, byte[] row) throws AvroBaseException {
    // Defaults
    byte[] latest = null;
    long timestamp = Long.MAX_VALUE;
    long version = -1;

    try {
      // Run through and find the latest value to get the bytes and the timestamp
      NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
      if (map != null) {
        NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = map.get(family);
        NavigableMap<Long, byte[]> values = familyData.get(DATA_COLUMN);
        if (values != null) {
          for (Map.Entry<Long, byte[]> e : values.entrySet()) {
            if (e.getKey() < timestamp) {
              latest = e.getValue();
            }
          }
        }

        // Grab the version
        byte[] versionB = result.getValue(family, VERSION_COLUMN);
        version = versionB == null ? -1 : Bytes.toLong(versionB);
      }
      // If the latest is null, just give up and return null
      if (latest != null) {
        // If not, load it up and return wrapped Row
        Schema schema = loadSchema(result);
        byte[] formatB = result.getValue(family, FORMAT_COLUMN);
        AvroFormat format = AvroFormat.BINARY;
        if (formatB != null) {
          format = AvroFormat.values()[Bytes.toInt(formatB)];
        }
        return new Row<T>(readValue(latest, schema, format), row, timestamp, version);
      }
      return null;
    } catch (IOException e) {
      throw new AvroBaseException(e);
    }
  }

  // Pull the version out of the version column. Version 0 means that it does not exist
  // in the hbase row

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

  // Ensure that this schema is present within the configured schema table

  private String storeSchema(Schema schema) throws AvroBaseException {
    String schemaKey;
    synchronized (schema) {
      schemaKey = hashCache.get(schema);
      if (schemaKey == null) {
        // Hash the schema, store it
        String doc = schema.toString();
        schemaKey = createSchemaKey(schema, doc);
        Put put = new Put($(schemaKey));
        put.add(AVRO_FAMILY, SCHEMA_COLUMN, $(doc));
        HTable schemaTable = pool.getTable(schemaName);
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

  // Pull an hbase row, ready to be wrapped by Row

  private Result getHBaseRow(HTable table, byte[] row, byte[] columnFamily) throws IOException {
    Get get = new Get(row);
    get.addColumn(columnFamily, DATA_COLUMN);
    get.addColumn(columnFamily, SCHEMA_COLUMN);
    get.addColumn(columnFamily, VERSION_COLUMN);
    get.addColumn(columnFamily, FORMAT_COLUMN);
    return table.get(get);
  }

  // Load a schema from the current hbase row

  private Schema loadSchema(Result result) throws AvroBaseException, IOException {
    byte[] row = result.getRow();
    byte[] schemaKey = result.getValue(family, SCHEMA_COLUMN);
    if (schemaKey == null) {
      throw new AvroBaseException("Schema not set for row: " + $_(row));
    }
    Schema schema = schemaCache.get($_(schemaKey));
    if (schema == null) {
      HTable schemaTable = pool.getTable(schemaName);
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

  // Get or create the specified table with columnfamily

  private HTable getTable(byte[] tableName, byte[] columnFamily) throws AvroBaseException {
    HTable table;
    try {
      table = pool.getTable(tableName);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof TableNotFoundException) {
        HColumnDescriptor family = getColumnDesc(columnFamily);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.addFamily(family);
        try {
          admin.createTable(tableDesc);
        } catch (IOException e1) {
          throw new AvroBaseException(e1);
        }
      } else {
        throw new AvroBaseException(e.getCause());
      }
      table = pool.getTable(tableName);
    }
    return table;
  }

  private HColumnDescriptor getColumnDesc(byte[] columnFamily) {
    HColumnDescriptor family = new HColumnDescriptor(columnFamily);
    family.setMaxVersions(1);
    family.setCompressionType(Compression.Algorithm.LZO);
    family.setInMemory(false);
    return family;
  }

}
