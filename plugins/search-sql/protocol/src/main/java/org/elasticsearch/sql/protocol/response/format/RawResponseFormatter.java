


package org.elasticsearch.sql.protocol.response.format;

/**
 * Response formatter to format response to csv or raw format.
 */
//@RequiredArgsConstructor
public class RawResponseFormatter extends FlatResponseFormatter {
  public RawResponseFormatter() {
    super("|", false);
  }

}
