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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
 *
 * Typically you would move all to S3 but only truncate after some delay for fast access to more recent data.
 * <p/>
 * User: sam
 * Date: 5/3/11
 * Time: 1:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class S3Archiver<T extends SpecificRecord> extends ForwardingAvroBase<T, byte[]> {
  private S3Service s3;
  private String bucket;
  private String path;
  private Schema actualSchema;

  @Inject
  public S3Archiver(AvroBase<T, byte[]> ab, Schema actualSchema, S3Service s3, String bucket, String path) {
    super(ab);
    this.s3 = s3;
    this.bucket = bucket;
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

          // S3
          private ArrayList<S3Object> files;
          private DataInputStream currentstream;
          private Boolean hasmore;

          // AvroBase
          private Schema schema;
          private byte[] row;

          @Override
          public synchronized boolean hasNext() {
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
              try {
                files = new ArrayList<S3Object>(newArrayList(filter(Arrays.asList(s3.listObjects(bucket, path, null)), new Predicate<S3Object>() {
                  public boolean apply(@Nullable S3Object input) {
                    return !input.getName().equals(path);
                  }
                })));
              } catch (S3ServiceException e) {
                throw new AvroBaseException("Failed to read files from S3 bucket", e);
              }
              Collections.sort(files, new Comparator<S3Object>() {
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
            }
            // If .next() hasn't been called return the previous answer
            if (hasmore != null) return hasmore;
            // Read the files in sequence, moving between them as they run out of data
            do {
              if (currentstream == null) {
                if (files.size() == 0) return hasmore = false;
                final S3Object nextFile = files.remove(0);
                try {
                  currentstream = new DataInputStream(new GZIPInputStream(s3.getObject(nextFile.getBucketName(), nextFile.getName()).getDataInputStream()));
                  final byte[] bytes = new byte[currentstream.readInt()];
                  currentstream.readFully(bytes);
                  schema = Schema.parse(new ByteArrayInputStream(bytes));
                } catch (ServiceException e) {
                  throw new AvroBaseException("Failed to read inputstream from S3: " + nextFile, e);
                } catch (IOException e) {
                  throw new AvroBaseException("Failed to read schema", e);
                }
              }
              try {
                do {
                  hasmore = currentstream.readBoolean();
                  if (hasmore) {
                    // We may be reading things that are supposed to be still in the
                    // local store but haven't been deleted yet
                    row = new byte[currentstream.readInt()];
                    currentstream.readFully(row);
                    if (stopRow != null) {
                      int compare = bytesComparator.compare(row, stopRow);
                      if (compare >= 0) {
                        currentstream.close();
                        currentstream = null;
                        return hasmore = false;
                      }
                    }
                    if (lastdelegatekey != null) {
                      final int compare = bytesComparator.compare(lastdelegatekey, row);
                      if (compare < 0) {
                        break;
                      } else {
                        // delete them from the local store or ignore them?
                        // skip for now
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

          @Override
          public synchronized Row<T, byte[]> next() {
            // Grab the next local value
            if (lastdelegatekey == null) return lastdelegaterow = iterator.next();
            // Grab the next S3 value
            if ((files.size() == 0 && currentstream == null) || (hasmore != null && !hasmore)) throw new NoSuchElementException();
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

  private String filename(S3Object s3Object) {
    return s3Object.getName().substring(path.length());
  }

  /**
   * Roll copies everything from startrow to the end to S3 in
   * a new file.
   *
   * @param startrow
   * @throws AvroBaseException
   */
  public void roll(byte[] startrow) throws AvroBaseException {
    File file = null;
    try {
      file = File.createTempFile("log", "gz");
      DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)));
      byte[] bytes = actualSchema.toString().getBytes(Charsets.UTF_8);
      dos.writeInt(bytes.length);
      dos.write(bytes);
      for (Row<T, byte[]> tRow : delegate().scan(startrow, null)) {
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
        S3Object s3o = new S3Object(path + new String(Hex.encodeHex(startrow)));
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
    }
  }

  /**
   * Truncate deletes everything after startrow. You only want to do this after
   * you have archived it all.
   * 
   * @param startrow
   */
  public void truncate(byte[] startrow) {
    for (Row<T, byte[]> tRow : delegate().scan(startrow, null)) {
      delegate().delete(tRow.row);
    }
  }
}
