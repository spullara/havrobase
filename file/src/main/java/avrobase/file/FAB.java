package avrobase.file;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Creator;
import avrobase.Mutator;
import avrobase.ReversableFunction;
import avrobase.Row;
import com.google.common.base.Supplier;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
public class FAB<T extends SpecificRecord, K> extends AvroBaseImpl<T, K> {

  private static final int HASH_LENGTH = 64;
  private static final int LONG_LENGTH = 8;
  private File dir;
  private File schemaDir;

  private final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<String, ReadWriteLock>();
  private Supplier<K> supplier;
  private ReversableFunction<K, byte[]> transformer;

  public FAB(String directory, String schemaDirectory, Supplier<K> supplier, Schema actualSchema, AvroFormat format, ReversableFunction<K, byte[]> transformer) {
    super(actualSchema, format);
    dir = new File(directory);
    dir.mkdirs();
    schemaDir = new File(schemaDirectory);
    schemaDir.mkdirs();
    this.supplier = supplier;
    this.transformer = transformer;
  }

  private String toFile(K row) {
    StringBuilder sb = new StringBuilder();
    String s = toString(row);
    if (s.length() > 2) {
      sb.append(s.substring(0, 2));
      sb.append("/");
      if (s.length() > 4) {
        sb.append(s.substring(2, 4));
        sb.append("/").append(s.substring(4));
      } else {
        sb.append(s.substring(2));
      }
      s = sb.toString();
    }
    return s;
  }

  private String toString(K row) {
    byte[] bytes = transformer.apply(row);
    return Hex.encodeHexString(bytes);
  }


  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    Lock lock = readLock(row);
    try {
      return _get(row);
    } catch (Exception e) {
      throw new AvroBaseException("Failed to get row: " + row, e);
    } finally {
      lock.unlock();
    }
  }

  private Lock readLock(K row) {
    ReadWriteLock readWriteLock = getLock(toString(row));
    Lock lock = readWriteLock.readLock();
    lock.lock();
    return lock;
  }

  private Row<T, K> _get(K row) throws IOException {
    File file = getFile(row, false);
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
        DecoderFactory decoderFactory = new DecoderFactory();
        Decoder d;
        switch (format) {
          case JSON:
            d = decoderFactory.jsonDecoder(schema, is);
            break;
          case BINARY:
          default:
            d = decoderFactory.binaryDecoder(is, null);
            break;
        }
        // Read the data
        SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema);
        sdr.setExpected(actualSchema);
        return new Row<T, K>(sdr.read(null, d), row, version);
      } catch (IOException e) {
        throw new AvroBaseException("Failed to read file: " + schema, e);
      } catch (AvroTypeException e) {
        throw new AvroBaseException("Failed to read value: " + schema, e);
      }
    } finally {
      channel.close();
      is.close();
    }
  }

  private Set<File> madedirs = new ConcurrentSkipListSet<File>();

  private File getFile(K row, boolean mkdirs) {
    File file = new File(dir, toFile(row));
    if (mkdirs) {
      File parentFile = file.getParentFile();
      if (!madedirs.contains(parentFile)) {
        madedirs.add(parentFile);
        parentFile.mkdirs();
      }
    }
    return file;
  }

  private ReadWriteLock getLock(String lockKey) {
    ReadWriteLock readWriteLock;
    synchronized (locks) {
      readWriteLock = locks.get(lockKey);
      if (readWriteLock == null) {
        readWriteLock = new ReentrantReadWriteLock();
        locks.put(lockKey, readWriteLock);
      }
    }
    return readWriteLock;
  }

  @Override
  public K create(T value) throws AvroBaseException {
    if (supplier == null) throw new AvroBaseException("No key generator provided");
    K row = supplier.get();
    put(row, value);
    return row;
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    Lock lock = writeLock(row);
    try {
      File file = getFile(row, true);
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

  private Lock writeLock(K row) {
    ReadWriteLock readWriteLock = getLock(toString(row));
    Lock lock = readWriteLock.writeLock();
    lock.lock();
    return lock;
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    Lock lock = writeLock(row);
    try {
      File file = getFile(row, true);
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
  public void delete(K row) throws AvroBaseException {
    Lock writeLock = writeLock(row);
    try {
      File file = getFile(row, false);
      FileInputStream fis = new FileInputStream(file);
      FileChannel channel = fis.getChannel();
      FileLock fileLock = channel.lock();
      try {
        file.delete();
      } finally {
        fileLock.release();
        channel.close();
        fis.close();
      }
    } catch (FileNotFoundException e) {
      // Already deleted
    } catch (IOException e) {
      throw new AvroBaseException("Failed to delete: " + row, e);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Iterable<Row<T, K>> scan(final K startRow, final K stopRow) throws AvroBaseException {
    final String start = startRow == null ? null : Hex.encodeHexString(transformer.apply(startRow));
    final String stop = stopRow == null ? null : Hex.encodeHexString(transformer.apply(stopRow));
    return new Iterable<Row<T, K>>() {
      @Override
      public Iterator<Row<T, K>> iterator() {
        return new Iterator<Row<T, K>>() {
          Queue<File> queue = createFileQueue(dir);
          Stack<Queue<File>> stack = new Stack<Queue<File>>();
          Stack<String> path = new Stack<String>();
          Row<T, K> current;

          @Override
          public synchronized boolean hasNext() {
            if (current != null) return true;
            do {
              File peek = queue.peek();
              if (peek == null) {
                if (stack.size() > 0) {
                  queue = stack.pop();
                  path.pop();
                  return hasNext();
                }
                return false;
              }
              File file = queue.poll();
              if (start != null || stop != null) {
                String p = getPath(file).toString();
                if (!include(p, start, stop)) continue;
              }
              if (file.isDirectory()) {
                path.push(file.getName());
                stack.push(queue);
                queue = createFileQueue(file);
              } else {
                StringBuilder sb = getPath(file);
                try {
                  current = get(transformer.unapply(Hex.decodeHex(sb.toString().toCharArray())));
                  return current != null || hasNext();
                } catch (DecoderException e) {
                  throw new AvroBaseException("Corrupt file system: " + file, e);
                }
              }
            } while (true);
          }

          private StringBuilder getPath(File file) {
            StringBuilder sb = new StringBuilder();
            for (String s : path) {
              sb.append(s);
            }
            sb.append(file.getName());
            return sb;
          }

          @Override
          public Row<T, K> next() {
            if (current == null) hasNext();
            if (current == null) throw new NoSuchElementException();
            Row<T, K> tmp = current;
            current = null;
            return tmp;
          }

          @Override
          public void remove() {
          }
        };
      }
    };
  }

  private LinkedList<File> createFileQueue(File startdir) {
    return new LinkedList<File>(Arrays.asList(startdir.listFiles()));
  }

  private boolean include(String s, String startRow, String stopRow) {
    return (startRow == null || s.compareTo(startRow) >= 0) && (stopRow == null || s.compareTo(stopRow) < 0);
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator) throws AvroBaseException {
    Lock writeLock = writeLock(row);
    try {
      File file = getFile(row, true);
      FileInputStream fis = new FileInputStream(file);
      FileChannel channel = fis.getChannel();
      FileLock fileLock = channel.lock();
      try {
        Row<T, K> tStringRow = _get(row);
        if (tStringRow == null) return null;
        T mutate = tMutator.mutate(tStringRow.value);
        if (mutate != null) {
          put(row, mutate);
          return new Row<T, K>(mutate, row);
        }
        return tStringRow;
      } finally {
        fileLock.release();
        channel.close();
        fis.close();
      }
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw new AvroBaseException("Failed to delete: " + row, e);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Row<T, K> mutate(K row, Mutator<T> tMutator, Creator<T> tCreator) throws AvroBaseException {
    Lock writeLock = writeLock(row);
    File file = getFile(row, true);
    try {
      FileInputStream fis = new FileInputStream(file);
      FileChannel channel = fis.getChannel();
      FileLock fileLock = channel.lock();
      try {
        Row<T, K> tStringRow = _get(row);
        if (tStringRow == null) return null;
        T mutate = tMutator.mutate(tStringRow.value);
        if (mutate != null) {
          put(row, mutate);
          return new Row<T, K>(mutate, row);
        }
        return tStringRow;
      } finally {
        fileLock.release();
        channel.close();
        fis.close();
      }
    } catch (FileNotFoundException e) {
      try {
        if (file.createNewFile()) {
          FileInputStream fis = new FileInputStream(file);
          FileChannel channel = fis.getChannel();
          FileLock fileLock = channel.lock();
          try {
            if (file.length() == 0) {
              T created = tCreator.create();
              put(row, created);
              return new Row<T, K>(created, row);
            }
          } finally {
            fileLock.release();
            channel.close();
            fis.close();
          }
        }
      } catch (IOException e1) {
        // Then do a normal mutate
      }
      return mutate(row, tMutator);
    } catch (IOException e) {
      throw new AvroBaseException("Failed to delete: " + row, e);
    } finally {
      writeLock.unlock();
    }
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
