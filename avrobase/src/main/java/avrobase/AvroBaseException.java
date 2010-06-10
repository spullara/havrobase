package avrobase;

import java.io.IOException;

/**
 * All checked exceptions in the system are wrapped by this one.
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:27:43 PM
 */
public class AvroBaseException extends RuntimeException {
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
