package avrobase.solr;

import avrobase.AvroBase;
import avrobase.IndexedAvroBase;
import avrobase.ReversableFunction;
import avrobase.Row;
import org.apache.avro.specific.SpecificRecord;

/**
 * Indexed avro base using solr.
 * <p/>
 * User: sam
 * Date: Sep 16, 2010
 * Time: 11:54:18 AM
 */
public class SolrAvroBase<T extends SpecificRecord, K> extends IndexedAvroBase<T, K, SQ> {
  public SolrAvroBase(final AvroBase<T, K> avroBase, SolrIndex<T, K> tkSolrIndex) {
    super(avroBase, tkSolrIndex);
  }

  public SolrAvroBase(final AvroBase<T, K> avroBase, String url, ReversableFunction<K, String> keyTx) {
    super(avroBase, new SolrIndex<T, K>(url, keyTx));
  }

  public void reindex() {
    final Iterable<Row<T, K>> rows = scan(null, null);
    for (Row<T, K> row : rows) {
      index.index(row);
    }
  }
}
