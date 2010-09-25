package avrobase.mysql;

import com.google.common.base.Function;
import org.apache.avro.specific.SpecificRecord;

public class ManyManyRelation<T extends SpecificRecord> {
  final Function<T,Long> leftFn;
  final String leftColName;
  final Function<T,Long> rightFn;
  final String rightColName;

  public ManyManyRelation(Function<T, Long> leftFn, String leftColName, Function<T, Long> rightFn, String rightColName) {
    this.leftFn = leftFn;
    this.leftColName = leftColName;
    this.rightFn = rightFn;
    this.rightColName = rightColName;
  }

  public Function<T, Long> getLeftFn() {
    return leftFn;
  }

  public String getLeftColName() {
    return leftColName;
  }

  public Function<T, Long> getRightFn() {
    return rightFn;
  }

  public String getRightColName() {
    return rightColName;
  }
}
