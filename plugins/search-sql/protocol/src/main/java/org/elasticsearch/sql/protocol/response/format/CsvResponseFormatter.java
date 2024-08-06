


package org.elasticsearch.sql.protocol.response.format;

public class CsvResponseFormatter extends FlatResponseFormatter {
  public CsvResponseFormatter() {
    super(",", true);
  }

  public CsvResponseFormatter(boolean sanitize) {
    super(",", sanitize);
  }

}
