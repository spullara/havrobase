package avrobase.s3archive;

import avrobase.AvroBase;
import avrobase.AvroBaseException;
import avrobase.ForwardingAvroBase;
import avrobase.Row;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.primitives.UnsignedBytes;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;

/**
 * The S3 Archiver will move entries from the delegate AvroBase to a log file format in S3. When
 * you scan and reach the end of the scan it will continue the scan on S3. There is a fundamental
 * assumption that the data stored in your delegate AvroBase is not updated and it is logically
 * at the end of the scan. No data is moved automatically. It is ok to temporarily have overlapping
 * data in S3, the archiver will fix it during its next scan.
 * <p/>
 * File format:
 * int - length of schema
 * bytes - schema
 * repeat:
 * boolean - has next
 * length, bytes - row
 * length, bytes - value
 * <p/>
 * Typically you would move all to S3 but only truncate after some delay for fast access to more recent data.
 * <p/>
 * User: sam
 * Date: 5/3/11
 * Time: 1:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class S3Archiver<T extends SpecificRecord> extends ForwardingAvroBase<T, byte[]> {
  protected S3Service s3;
  private String bucket;
  private boolean reverse;
  private String path;
  private Schema actualSchema;

  @Inject
  public S3Archiver(AvroBase<T, byte[]> ab, Schema actualSchema, S3Service s3, String bucket, String path, boolean reverse) {
    super(ab);
    this.s3 = s3;
    this.bucket = bucket;
    this.reverse = reverse;
    this.path = path.endsWith("/") ? path : path + "/";
    this.actualSchema = actualSchema;
  }

  /**
   * Get calls the underlying delegate directly and does NOT scan S3 to find something
   * that is not present the store of record. The only operation that uses the S3 store
   * is scan.
   *
   * @param row
   * @return
   * @throws AvroBaseException
   */
  @Override
  public Row<T, byte[]> get(byte[] row) throws AvroBaseException {
    return super.get(row);
  }

  private Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();
  private static DecoderFactory decoderFactory = new DecoderFactory();
  private static EncoderFactory encoderFactory = new EncoderFactory();

  @Override
  public Iterable<Row<T, byte[]>> scan(byte[] startRow, final byte[] stopRow) throws AvroBaseException {
    final Iterable<Row<T, byte[]>> scan = delegate().scan(startRow, stopRow);
    return new Iterable<Row<T, byte[]>>() {
      @Override
      public Iterator<Row<T, byte[]>> iterator() {
        final Iterator<Row<T, byte[]>> iterator = scan.iterator();
        return new Iterator<Row<T, byte[]>>() {
          // Local
          private byte[] lastdelegatekey;
          private Row<T, byte[]> lastdelegaterow;
          boolean firstrow = false;
          public byte[] firstdelegatekey;
          private Row<T, byte[]> firstdelegaterow;

          // S3
          private ArrayList<S3Object> files;
          private DataInputStream currentstream;
          private Boolean hasmore;

          // AvroBase
          private Schema schema;
          private byte[] row;

          @Override
          public synchronized boolean hasNext() {
            if (reverse) {
              // Scan what we have locally
              if (iterator.hasNext()) {
                firstdelegaterow = iterator.next();
                firstdelegatekey = firstdelegaterow == null ? null : firstdelegaterow.row;
              }
              return startArchived();
            } else {
              return stopArchived();
            }
          }

          private boolean stopArchived() {
            // Scan what we have locally
            if (iterator.hasNext()) {
              return true;
            } else if (lastdelegatekey == null) {
              lastdelegatekey = lastdelegaterow == null ? null : lastdelegaterow.row;
              lastdelegaterow = null;
            }
            if (lastdelegatekey != null && stopRow != null && bytesComparator.compare(lastdelegatekey, stopRow) >= 0) {
              return false;
            }
            // Scan S3
            if (files == null) {
              files = getArchives();
            }
            // If .next() hasn't been called return the previous answer
            if (hasmore != null) return hasmore;
            // Read the files in sequence, moving between them as they run out of data
            do {
              if (assertCurrentStream()) return hasmore = false;
              try {
                do {
                  hasmore = currentstream.readBoolean();
                  if (hasmore) {
                    // We may be reading things that are supposed to be still in the
                    // local store but haven't been deleted yet
                    if (nextRowInStream()) return hasmore = false;
                    if (lastdelegatekey != null) {
                      // skip archived rows we have already scanned
                      final int compare = bytesComparator.compare(lastdelegatekey, row);
                      if (compare < 0) {
                        break;
                      } else {
                        currentstream.readFully(new byte[currentstream.readInt()]);
                      }
                    } else {
                      break;
                    }
                  } else {
                    currentstream.close();
                    currentstream = null;
                  }
                } while (hasmore);
              } catch (EOFException e) {
                e.printStackTrace();
                return hasmore = false;
              } catch (IOException e) {
                throw new AvroBaseException("Failed to read s3 object stream", e);
              }
            } while (!hasmore);
            return hasmore;
          }

          private boolean startArchived() {
            // Scan S3
            if (files == null) {
              files = getArchives();
            }
            // If .next() hasn't been called return the previous answer
            if (hasmore != null) return hasmore;
            // Read the files in sequence, moving between them as they run out of data
            if (firstdelegaterow != null) {
              do {
                if (assertCurrentStream()) return hasmore = false;
                try {
                  do {
                    hasmore = currentstream.readBoolean();
                    if (hasmore) {
                      if (nextRowInStream()) return hasmore = false;

                      if (firstdelegatekey != null) {
                        // skip archived rows we have already scanned
                        final int compare = bytesComparator.compare(firstdelegatekey, row);
                        if (compare >= 0) {
                          break;
                        } else {
                          currentstream.readFully(new byte[currentstream.readInt()]);
                        }
                      } else {
                        break;
                      }
                    } else {
                      currentstream.close();
                      currentstream = null;
                    }
                  } while (hasmore);
                } catch (EOFException e) {
                  e.printStackTrace();
                  return hasmore = false;
                } catch (IOException e) {
                  throw new AvroBaseException("Failed to read s3 object stream", e);
                }
              } while (!hasmore);
            } else {
              hasmore = false;
            }
            if (!hasmore) {
              if (stopRow != null && bytesComparator.compare(row, stopRow) >= 0) {
                return false;
              }
              if (firstdelegaterow != null) {
                firstrow = true;
                return hasmore = true;
              }
              return hasmore = iterator.hasNext();
            }
            return hasmore;
          }

          private boolean nextRowInStream() throws IOException {
            // We may be reading things that are supposed to be still in the
            // local store but haven't been deleted yet
            row = new byte[currentstream.readInt()];
            currentstream.readFully(row);
            if (stopRow != null) {
              int compare = bytesComparator.compare(row, stopRow);
              if (compare >= 0) {
                currentstream.close();
                currentstream = null;
                return true;
              }
            }
            return false;
          }

          private boolean assertCurrentStream() {
            if (currentstream == null) {
              if (files.size() == 0) return true;
              final S3Object nextFile = files.remove(0);
              try {
                currentstream = new DataInputStream(new GZIPInputStream(getInputStream(nextFile)));
                final byte[] bytes = new byte[currentstream.readInt()];
                currentstream.readFully(bytes);
                schema = Schema.parse(new ByteArrayInputStream(bytes));
              } catch (ServiceException e) {
                throw new AvroBaseException("Failed to read inputstream from S3: " + nextFile, e);
              } catch (IOException e) {
                throw new AvroBaseException("Failed to read schema", e);
              }
            }
            return false;
          }

          @Override
          public synchronized Row<T, byte[]> next() {
            // Grab the next local value
            if (reverse) {
              if (firstrow) {
                try {
                  firstrow = false;
                  hasmore = null;
                  return firstdelegaterow;
                } finally {
                  firstdelegaterow = null;
                }
              }
              if (firstdelegaterow == null) {
                return iterator.next();
              }
            } else {
              if (lastdelegatekey == null) return lastdelegaterow = iterator.next();
            }
            // Grab the next S3 value
            if ((files.size() == 0 && currentstream == null) || (hasmore != null && !hasmore))
              throw new NoSuchElementException();
            hasmore = null;
            try {
              byte[] bytes = new byte[currentstream.readInt()];
              currentstream.readFully(bytes);
              SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema, actualSchema);
              T read = sdr.read(null, decoderFactory.binaryDecoder(bytes, null));
              return new Row<T, byte[]>(read, row);
            } catch (IOException e) {
              throw new AvroBaseException("Invalid data in log", e);
            }
          }

          @Override
          public void remove() {
          }
        };
      }
    };
  }

  protected InputStream getInputStream(S3Object nextFile) throws ServiceException, IOException {
    return s3.getObject(nextFile.getBucketName(), nextFile.getName()).getDataInputStream();
  }

  private ArrayList<S3Object> getArchives() {
    ArrayList<S3Object> files1;
    try {
      files1 = new ArrayList<S3Object>(newArrayList(filter(Arrays.asList(s3.listObjects(bucket, path, null)), new Predicate<S3Object>() {
        public boolean apply(@Nullable S3Object input) {
          return !input.getName().equals(path);
        }
      })));
    } catch (S3ServiceException e) {
      throw new AvroBaseException("Failed to read files from S3 bucket", e);
    }
    Collections.sort(files1, new Comparator<S3Object>() {
      @Override
      public int compare(S3Object s3Object, S3Object s3Object1) {
        try {
          final byte[] key = Hex.decodeHex(filename(s3Object).toCharArray());
          final byte[] key1 = Hex.decodeHex(filename(s3Object1).toCharArray());
          return bytesComparator.compare(key, key1);
        } catch (DecoderException e) {
          throw new AvroBaseException("Failed to decode filename: " + s3Object.getName(), e);
        }
      }
    });
    return files1;
  }

  private String filename(S3Object s3Object) {
    return s3Object.getName().substring(path.length());
  }

  /**
   * Roll copies everything from startrow to the end to S3 in
   * a new file.
   *
   * @param row
   * @throws AvroBaseException
   */
  public void roll(byte[] row) throws AvroBaseException {
    // Find the entry in the archives
    List<S3Object> archives = getArchives();
    byte[] archiveRow;
    if (archives.size() > 0) {
      try {
        if (reverse) {
          archiveRow = Hex.decodeHex(archives.get(archives.size() - 1).getName().substring(path.length()).toCharArray());
        } else {
          archiveRow = Hex.decodeHex(archives.get(0).getName().substring(path.length()).toCharArray());
        }
      } catch (DecoderException e) {
        throw new AvroBaseException("Failed to get row from archives: " + archives, e);
      }
      if (reverse) {
        if (bytesComparator.compare(row, archiveRow) < 0) {
          return;
        }
      } else {
        if (bytesComparator.compare(row, archiveRow) >= 0) {
          return;
        }
      }
    } else {
      archiveRow = null;
    }
    File file = null;
    try {
      file = File.createTempFile("log", "gz");
      DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)));
      byte[] bytes = actualSchema.toString().getBytes(Charsets.UTF_8);
      dos.writeInt(bytes.length);
      dos.write(bytes);
      for (Row<T, byte[]> tRow : getScanner(row)) {
        if (archiveRow != null) {
          if (reverse) {
            if (bytesComparator.compare(tRow.row, archiveRow) < 0) {
              break;
            }
          } else {
            if (bytesComparator.compare(tRow.row, archiveRow) >= 0) {
              break;
            }
          }
        }
        dos.writeBoolean(true);
        dos.writeInt(tRow.row.length);
        dos.write(tRow.row);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder be = encoderFactory.binaryEncoder(baos, null);
        SpecificDatumWriter<T> sdw = new SpecificDatumWriter<T>(actualSchema);
        sdw.write(tRow.value, be);
        be.flush();
        bytes = baos.toByteArray();
        dos.writeInt(bytes.length);
        dos.write(bytes);
      }
      dos.writeBoolean(false);
      dos.flush();
      dos.close();
      // Retry 3 times
      for (int i = 0; i < 3; i++) {
        S3Object s3o = new S3Object(path + new String(Hex.encodeHex(row)));
        s3o.setContentLength(file.length());
        s3o.setDataInputStream(new BufferedInputStream(new FileInputStream(file)));
        s3o.setContentType("application/gzip");
        try {
          s3.putObject(bucket, s3o);
          break;
        } catch (S3ServiceException e) {
          if (i == 2) {
            throw new AvroBaseException("Failed to upload to S3", e);
          }
        }
      }
    } catch (IOException e) {
      throw new AvroBaseException("Failed to read/write file: " + file, e);
    } finally {
      if (file != null) {
        file.delete();
      }
    }
  }

  /**
   * Truncate deletes everything after row (or before row). You only want to do this after
   * you have archived it all.
   *
   * @param row
   */
  public void truncate(byte[] row) {
    final Iterable<Row<T, byte[]>> scan = getScanner(row);
    for (Row<T, byte[]> tRow : scan) {
      delegate().delete(tRow.row);
    }
  }

  private Iterable<Row<T, byte[]>> getScanner(byte[] row) {
    final Iterable<Row<T, byte[]>> scan;
    if (reverse) {
      scan = delegate().scan(null, row);
    } else {
      scan = delegate().scan(row, null);
    }
    return scan;
  }
}
