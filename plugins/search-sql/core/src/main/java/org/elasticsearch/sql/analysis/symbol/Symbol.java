

package org.elasticsearch.sql.analysis.symbol;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Symbol in the scope.
 */
@ToString
@Getter
@RequiredArgsConstructor
public class Symbol {
  private final Namespace namespace;
  private final String name;
}
