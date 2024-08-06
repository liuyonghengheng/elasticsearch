


package org.elasticsearch.sql.legacy.cursor;

/**
 * A placeholder Cursor implementation to work with non-paginated queries.
 */
public class NullCursor implements Cursor {

    private final CursorType type = CursorType.NULL;

    @Override
    public String generateCursorId() {
        return null;
    }

    @Override
    public CursorType getType() {
        return type;
    }

    public NullCursor from(String cursorId) {
        return NULL_CURSOR;
    }
}
