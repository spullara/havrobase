package avrobase.file;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Creator;
import avrobase.Mutator;
import avrobase.Row;
import avrobase.shard.ShardableAvroBase;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * File based avrobase.
 * <p/>
 * User: sam
 * Date: 10/10/10
 * Time: 4:08 PM
 */
public class FAB<T extends SpecificRecord> extends AvroBaseImpl<T, String> implements ShardableAvroBase<T, String> {

  private static final int HASH_LENGTH = 64;
  private static final int LONG_LENGTH = 8;
  private File dir;
  private File schemaDir;

  private final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<String, ReadWriteLock>();
  private Supplier<String> kg;

  public FAB(String directory, String schemaDirectory, Schema actualSchema, AvroFormat format) {
    super(actualSchema, format);
    dir = new File(directory);
    dir.mkdirs();
    schemaDir = new File(schemaDirectory);
    schemaDir.mkdirs();
  }

  public FAB(String directory, String schemaDirectory, Supplier<String> kg, Schema actualSchema, AvroFormat format) {
    this(directory, schemaDirectory, actualSchema, format);
    this.kg = kg;
  }

  @Override
  public byte[] representation() throws AvroBaseException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(baos));
    try {
      bw.write(dir.getCanonicalPath());
      bw.write("\n");
      bw.write(schemaDir.getCanonicalPath());
      bw.write("\n");
      bw.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new AvroBaseException("Failed to create representation", e);
    }
  }

  @Override
  public void init(byte[] representation) throws AvroBaseException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(representation)));
    try {
      dir = new File(br.readLine());
      schemaDir = new File(br.readLine());
      br.close();
    } catch (IOException e) {
      throw new AvroBaseException("Could not read representation", e);
    }
    dir.mkdirs();
    schemaDir.mkdirs();
  }

  @Override
  public Iterable<String> scanKeys(final String start, final String end) throws AvroBaseException {
    return Iterables.transform(Arrays.asList(dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String s) {
        return s.compareTo(start) >= 0 && s.compareTo(end) < 0;
      }
    })), new Function<File, String>() {
      @Override
      public String apply(@Nullable File input) {
        return input.getName();
      }
    });
  }

  @Override
  public Row<T, String> get(String row) throws AvroBaseException {
    ReadWriteLock readWriteLock = getLock(row);
    Lock lock = readWriteLock.readLock();
    lock.lock();
    try {
      File file = new File(dir, row);
      FileInputStream fis = new FileInputStream(file);
      FileChannel channel = fis.getChannel();
      // Lock the file on disk
      InputStream is = new BufferedInputStream(fis);
      try {
        // Read the hash of the schema
        byte[] bytes = new byte[HASH_LENGTH];
        int total = 0;
        int read;
        while (total != HASH_LENGTH && (read = is.read(bytes, total, HASH_LENGTH - total)) != -1) {
          total += read;
        }
        String hash = new String(bytes);
        // Read the version of the object
        bytes = new byte[8];
        total = 0;
        while (total != LONG_LENGTH && (read = is.read(bytes, total, LONG_LENGTH - total)) != -1) {
          total += read;
        }
        long version = ByteBuffer.wrap(bytes).getLong();
        // Get the schema
        Schema schema = schemaCache.get(hash);
        if (schema == null) {
          File schemaFile = new File(schemaDir, hash);
          schema = Schema.parse(new FileInputStream(schemaFile));
          schemaCache.put(hash, schema);
          hashCache.put(schema, hash);
        }
        try {
          Decoder d;
          switch (format) {
            case JSON:
              d = new JsonDecoder(schema, is);
              break;
            case BINARY:
            default:
              DecoderFactory factory = new DecoderFactory();
              d = factory.createBinaryDecoder(is, null);
              break;
          }
          // Read the data
          SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema);
          sdr.setExpected(actualSchema);
          return new Row<T, String>(sdr.read(null, d), row, version);
        } catch (IOException e) {
          throw new AvroBaseException("Failed to read file: " + schema, e);
        } catch (AvroTypeException e) {
          throw new AvroBaseException("Failed to read value: " + schema, e);
        }
      } finally {
        channel.close();
        is.close();
      }
    } catch (Exception e) {
      throw new AvroBaseException("Failed to get row: " + row, e);
    } finally {
      lock.unlock();
    }
  }

  private ReadWriteLock getLock(String row) {
    ReadWriteLock readWriteLock;
    synchronized (locks) {
      readWriteLock = locks.get(row);
      if (readWriteLock == null) {
        readWriteLock = new ReentrantReadWriteLock();
        locks.put(row, readWriteLock);
      }
    }
    return readWriteLock;
  }

  @Override
  public String create(T value) throws AvroBaseException {
    if (kg == null) throw new AvroBaseException("No key generator provided");
    String row = kg.get();
    put(row, value);
    return row;
  }

  @Override
  public void put(String row, T value) throws AvroBaseException {
    ReadWriteLock readWriteLock = getLock(row);
    Lock lock = readWriteLock.writeLock();
    lock.lock();
    try {
      File file = new File(dir, row);
      RandomAccessFile raf = new RandomAccessFile(file, "rw");
      FileChannel channel = raf.getChannel();
      FileLock fileLock = channel.lock();
      try {
        Schema schema = value.getSchema();
        String hash = hashCache.get(schema);
        if (hash == null) {
          String doc = schema.toString();
          hash = createSchemaKey(schema, doc);
          File schemaFile = new File(schemaDir, hash);
          if (!schemaFile.exists()) {
            FileOutputStream schemaOs = new FileOutputStream(schemaFile);
            schemaOs.write(doc.getBytes());
            schemaOs.close();
          }
        }
        raf.write(hash.getBytes());
        long version = 1;
        if (file.exists()) {
          byte[] bytes = new byte[8];
          int total = 0;
          int read;
          while (total != LONG_LENGTH && (read = raf.read(bytes, total, LONG_LENGTH - total)) != -1) {
            total += read;
          }
          version = ByteBuffer.wrap(bytes).getLong() + 1;
          raf.seek(HASH_LENGTH);
        }
        raf.write(ByteBuffer.wrap(new byte[8]).putLong(version).array());
        raf.write(serialize(value));
      } finally {
        channel.force(false);
        fileLock.release();
        channel.close();
        raf.close();
      }
    } catch (Exception e) {
      throw new AvroBaseException("Failed to get row: " + row, e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean put(String row, T value, long version) throws AvroBaseException {
    ReadWriteLock readWriteLock = getLock(row);
    Lock lock = readWriteLock.writeLock();
    lock.lock();
    try {
      File file = new File(dir, row);
      if (version != 0 && !file.exists()) return false;
      if (version == 0 && file.exists()) return false;
      RandomAccessFile raf = new RandomAccessFile(file, "rw");
      FileChannel channel = raf.getChannel();
      FileLock fileLock = channel.lock();
      try {
        Schema schema = value.getSchema();
        if (file.exists()) {
          raf.seek(HASH_LENGTH);
          byte[] bytes = new byte[8];
          int total = 0;
          int read;
          while (total != LONG_LENGTH && (read = raf.read(bytes, total, LONG_LENGTH - total)) != -1) {
            total += read;
          }
          long saved = ByteBuffer.wrap(bytes).getLong();
          if (saved != version) {
            return false;
          }
        }
        String hash = hashCache.get(schema);
        if (hash == null) {
          String doc = schema.toString();
          hash = createSchemaKey(schema, doc);
          File schemaFile = new File(schemaDir, hash);
          if (!schemaFile.exists()) {
            FileOutputStream schemaOs = new FileOutputStream(schemaFile);
            schemaOs.write(doc.getBytes());
            schemaOs.close();
          }
        }
        raf.seek(0);
        raf.write(hash.getBytes());
        raf.write(ByteBuffer.wrap(new byte[8]).putLong(version + 1).array());
        raf.write(serialize(value));
        return true;
      } finally {
        channel.force(false);
        fileLock.release();
        channel.close();
        raf.close();
      }
    } catch (Exception e) {
      throw new AvroBaseException("Failed to get row: " + row, e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void delete(String row) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Row<T, String>> scan(String startRow, String stopRow) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Row<T, String> mutate(String row, Mutator<T> tMutator) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Row<T, String> mutate(String row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    throw new NotImplementedException();
  }

  static final byte[] HEX_CHAR_TABLE = {
      (byte) '0', (byte) '1', (byte) '2', (byte) '3',
      (byte) '4', (byte) '5', (byte) '6', (byte) '7',
      (byte) '8', (byte) '9', (byte) 'a', (byte) 'b',
      (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
  };

  public static String getHexString(byte[] raw) {
    byte[] hex = new byte[2 * raw.length];
    int index = 0;

    for (byte b : raw) {
      int v = b & 0xFF;
      hex[index++] = HEX_CHAR_TABLE[v >>> 4];
      hex[index++] = HEX_CHAR_TABLE[v & 0xF];
    }
    return new String(hex);
  }

}
