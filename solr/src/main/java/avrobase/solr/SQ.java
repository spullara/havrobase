package avrobase.solr;

import org.apache.solr.client.solrj.SolrQuery;

/**
 * The default query generator for SolrAvroBase.
 * <p/>
 * User: sam
 * Date: Aug 8, 2010
 * Time: 12:58:21 PM
 */
public class SQ {

  public SQ(String query) {
    this.query = query;
  }

  public SQ(String query, int start, int count) {
    this.query = query;
    this.start = start;
    this.count = count;
  }

  public SQ(String query, SortField[] sort) {
    this.query = query;
    this.sort = sort;
  }

  public SQ(String query, SortField[] sort, int start, int count) {
    this.query = query;
    this.sort = sort;
    this.start = start;
    this.count = count;
  }

  public SolrQuery generateQuery(String uniqueKey) {
    SolrQuery solrQuery = new SolrQuery().setQuery(query).setStart(start).setRows(count).setFields(uniqueKey);
    if (sort != null) {
      for (SortField sf : sort) {
        solrQuery.addSortField(sf.field, sf.order);
      }
    }
    return solrQuery;
  }

  public static class SortField {
    public SortField(String field, SolrQuery.ORDER order) {
      this.field = field;
      this.order = order;
    }
    public String field;
    public SolrQuery.ORDER order;
  }

  protected String query;
  protected SortField[] sort;
  protected int start = 0;
  protected int count = 10;
}
