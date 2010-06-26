package avrobase;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class with built in serialization and deserialization and schema management.
 * <p/>
 * User: sam
 * Date: Jun 23, 2010
 * Time: 12:21:33 PM
 */
public abstract class AvroBaseImpl<T extends SpecificRecord> implements AvroBase<T> {
  private static final String SCHEMA_LOCATION = "/admin/file/?file=schema.xml";

  protected Map<String, Schema> schemaCache = new ConcurrentHashMap<String, Schema>();
  protected Map<Schema, String> hashCache = new ConcurrentHashMap<Schema, String>();
  protected AvroFormat format;

  // Search
  protected SolrServer solrServer;
  protected String uniqueKey;
  protected List<String> fields;

  protected static final Charset UTF8 = Charset.forName("utf-8");

  public AvroBaseImpl(AvroFormat format) {
    this.format = format;
  }

  public AvroBaseImpl(AvroFormat format, String solrURL) throws AvroBaseException {
    this(format);
    if (solrURL != null) {
      try {
        solrServer = new CommonsHttpSolrServer(solrURL);
        URL url = new URL(solrURL + SCHEMA_LOCATION);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(url.openStream());

        // Need to get the unique key and all the fields
        NodeList uniqueKeys = document.getElementsByTagName("uniqueKey");
        if (uniqueKeys == null || uniqueKeys.getLength() != 1) {
          throw new AvroBaseException("Invalid schema configuration, must have 1 unique key");
        }
        uniqueKey = uniqueKeys.item(0).getTextContent();

        // Now get all the fields we are going to index and query
        NodeList fieldList = document.getElementsByTagName("field");
        fields = new ArrayList<String>(fieldList.getLength());
        for (int i = 0; i < fieldList.getLength(); i++) {
          Node field = fieldList.item(i);
          String name = field.getAttributes().getNamedItem("name").getTextContent();
          fields.add(name);
        }
      } catch (MalformedURLException e) {
        throw new AvroBaseException("Invalid Solr URL: " + solrURL, e);
      } catch (ParserConfigurationException e) {
        throw new AvroBaseException(e);
      } catch (SAXException e) {
        throw new AvroBaseException("Failed to parse schema", e);
      } catch (IOException e) {
        throw new AvroBaseException("Failed to read schema", e);
      }
    }
  }

  @Override
  public Iterable<Row<T>> search(String query, int start, int rows) throws AvroBaseException {
    if (solrServer == null) {
      throw new AvroBaseException("Searching for this type is not enabled");
    }
    SolrQuery solrQuery = new SolrQuery().setQuery(query).setStart(start).setRows(rows).setFields(uniqueKey);
    try {
      QueryResponse queryResponse = solrServer.query(solrQuery);
      SolrDocumentList list = queryResponse.getResults();
      final Iterator<SolrDocument> solrDocumentIterator = list.iterator();
      return new Iterable<Row<T>>() {

        @Override
        public Iterator<Row<T>> iterator() {
          return new Iterator<Row<T>>() {

            @Override
            public boolean hasNext() {
              return solrDocumentIterator.hasNext();
            }

            @Override
            public Row<T> next() {
              SolrDocument solrDocument = solrDocumentIterator.next();
              Map<String, Object> map = solrDocument.getFieldValueMap();
              Object o = map.get(uniqueKey);
              if (o == null) {
                throw new AvroBaseException("Unique key not present in document");
              }
              return get($(o.toString()));
            }

            @Override
            public void remove() {
              throw new NotImplementedException();
            }
          };
        }
      };
    } catch (SolrServerException e) {
      throw new AvroBaseException("Query failure: " + query, e);
    }
  }

  /**
   * Index a row and value.
   * @param row
   * @param value
   * @return
   */
  protected boolean index(byte[] row, T value) {
    if (solrServer == null) {
      return false;
    }
    Schema schema = value.getSchema();
    SolrInputDocument document = new SolrInputDocument();
    for (String field : fields) {
      Schema.Field f = schema.getField(field);
      if (f != null) {
        Object o = value.get(f.pos());
        document.addField(field, o);
      }
    }
    document.addField(uniqueKey, $_(row));
    try {
      UpdateRequest req = new UpdateRequest();
      req.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
      req.add(document);
      solrServer.request(req);
      return true;
    } catch (SolrServerException e) {
      throw new AvroBaseException(e);
    } catch (IOException e) {
      throw new AvroBaseException(e);
    }
  }

  /**
   * Reindex all rows.  Could be very very expensive.
   */
  protected void reindex() {
    for (Row<T> tRow: scan(null, null)) {
      index(tRow.row, tRow.value);
    }
  }

  /**
   * Load a schema from the schema table
   */
  protected Schema loadSchema(byte[] value, String row) throws AvroBaseException {
    Schema schema = null;
    try {
      schema = Schema.parse(new ByteArrayInputStream(value));
    } catch (IOException e) {
      throw new AvroBaseException("Failed to deserialize schema: " + row, e);
    }
    schemaCache.put(row, schema);
    hashCache.put(schema, row);
    return schema;
  }

  /**
   * Serialize the Avro instance using its schema and the
   * format set for this avrobase
   */
  protected byte[] serialize(T value) throws AvroBaseException {
    try {
      Schema schema = value.getSchema();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Encoder be;
      switch (format) {
        case JSON:
          be = new JsonEncoder(schema, baos);
          break;
        case BINARY:
        default:
          be = new BinaryEncoder(baos);
          break;
      }
      SpecificDatumWriter<T> sdw = new SpecificDatumWriter<T>(schema);
      sdw.write(value, be);
      be.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new AvroBaseException("Failed to serialize", e);
    }
  }

  protected String createSchemaKey(Schema schema, String doc) {
    String schemaKey;
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      md = null;
    }
    if (md == null) {
      schemaKey = doc;
    } else {
      schemaKey = new String(Hex.encodeHex(md.digest(doc.getBytes())));
    }
    schemaCache.put(schemaKey, schema);
    hashCache.put(schema, schemaKey);
    return schemaKey;
  }

  /**
   * Read the avro serialized data using the specified schema and format
   * in the hbase row
   */
  protected T readValue(byte[] latest, Schema schema, AvroFormat format) throws AvroBaseException {
    try {
      Decoder d;
      switch (format) {
        case JSON:
          d = new JsonDecoder(schema, new ByteArrayInputStream(latest));
          break;
        case BINARY:
        default:
          DecoderFactory factory = new DecoderFactory();
          d = factory.createBinaryDecoder(new ByteArrayInputStream(latest), null);
          break;
      }
      SpecificDatumReader<T> sdr = new SpecificDatumReader<T>(schema);
      return sdr.read(null, d);
    } catch (IOException e) {
      throw new AvroBaseException("Failed to read value", e);
    }
  }

  protected static byte[] $(String string) {
    return string.getBytes(UTF8);
  }

  protected static String $_(byte[] bytes) {
    return new String(bytes, UTF8);
  }
}
