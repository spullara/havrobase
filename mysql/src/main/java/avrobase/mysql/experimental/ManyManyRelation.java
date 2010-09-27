package avrobase.mysql.experimental;

import org.apache.avro.specific.SpecificRecord;

public class ManyManyRelation<T extends SpecificRecord> {
  private final ReferenceColumn<T> left;
  private final ReferenceColumn<T> right;

  public ManyManyRelation(ReferenceColumn<T> left, ReferenceColumn<T> right) {
    this.left = left;
    this.right = right;
  }

  public ReferenceColumn<T> getLeft() {
    return left;
  }

  public ReferenceColumn<T> getRight() {
    return right;
  }
}
