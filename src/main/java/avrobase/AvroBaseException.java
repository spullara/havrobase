package avrobase;

import java.io.IOException;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:27:43 PM
 */
public class AvroBaseException extends Throwable {
  public AvroBaseException(Throwable cause) {
    super(cause);
  }

  public AvroBaseException(String message) {
    super(message);
  }

  public AvroBaseException(String s, IOException e) {
    super(s, e);
  }
}
