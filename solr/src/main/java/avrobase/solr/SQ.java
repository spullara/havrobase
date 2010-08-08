package avrobase.solr;

import org.apache.solr.client.solrj.SolrQuery;

/**
 * TODO: Edit this
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
  public static class SortField {
    public String field;
    public SolrQuery.ORDER order;
  }

  public String query;
  public SortField[] sort;
  public int start = 0;
  public int count = 0;
}
