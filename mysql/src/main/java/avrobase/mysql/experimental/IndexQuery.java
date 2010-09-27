package avrobase.mysql.experimental;

public class IndexQuery<V> {
  private final IndexColumn column;
  private final V value;

  public IndexQuery(IndexColumn column, V value) {
    this.column = column;
    this.value = value;
  }

  public IndexColumn getColumn() {
    return column;
  }

  public V getValue() {
    return value;
  }
}
