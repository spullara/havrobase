package havrobase;

import bagcheck.GenderType;
import bagcheck.User;
import junit.framework.TestCase;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Check reading and writing User objects
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 3:27:24 PM
 */
public class UserTest extends TestCase {

  public void testSave() throws IOException {
    File file = File.createTempFile("hbase", "avro");
    User saved = new User();
    {
      saved.firstName = $("Sam");
      saved.lastName = $("Pullara");
      saved.birthday = $("1212");
      saved.gender = GenderType.MALE;
      saved.email = $("spullara@yahoo.com");
      saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
      saved.title = $("Engineer");
      saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
      saved.location = $("Los Altos, CA");
      saved.password = ByteBuffer.wrap($("").getBytes());
      FileOutputStream fos = new FileOutputStream(file);
      BinaryEncoder be = new BinaryEncoder(fos);
      SpecificDatumWriter<User> sdw = new SpecificDatumWriter<User>(User.class);
      sdw.write(saved, be);
      be.flush();
      fos.close();
    }
    {
      FileInputStream fis = new FileInputStream(file);
      DecoderFactory factory = new DecoderFactory();
      BinaryDecoder bd = factory.createBinaryDecoder(fis, null);
      SpecificDatumReader<User> sdr = new SpecificDatumReader<User>(User.class);
      User loaded = sdr.read(null, bd);
      fis.close();
      assertEquals(saved, loaded);
      assertEquals("Sam", loaded.firstName.toString());
    }
  }

  public void testOptional() throws IOException {
    File file = File.createTempFile("hbase", "avro");
    User saved = new User();
    {
      saved.firstName = $("Sam");
      saved.lastName = $("Pullara");
//      saved.birthday = $("1212");
//      saved.gender = GenderType.MALE;
      saved.email = $("spullara@yahoo.com");
//      saved.description = $("CTO of RightTime, Inc. and one of the founders of BagCheck");
//      saved.title = $("Engineer");
      saved.image = $("http://farm1.static.flickr.com/1/buddyicons/32354567@N00.jpg");
//      saved.location = $("Los Altos, CA");
      saved.password = ByteBuffer.wrap($("").getBytes());
      FileOutputStream fos = new FileOutputStream(file);
      BinaryEncoder be = new BinaryEncoder(fos);
      SpecificDatumWriter<User> sdw = new SpecificDatumWriter<User>(User.class);
      sdw.write(saved, be);
      be.flush();
      fos.close();
    }
    {
      FileInputStream fis = new FileInputStream(file);
      DecoderFactory factory = new DecoderFactory();
      BinaryDecoder bd = factory.createBinaryDecoder(fis, null);
      SpecificDatumReader<User> sdr = new SpecificDatumReader<User>(User.class);
      User loaded = sdr.read(null, bd);
      fis.close();
      assertEquals(saved, loaded);
      assertEquals(null, loaded.location);
      assertEquals("Sam", loaded.firstName.toString());
    }
  }

  private Utf8 $(String value) {
    return new Utf8(value);
  }
}
