


package org.elasticsearch.sql.legacy.antlr.semantic.scope;

/**
 * Symbol in the scope
 */
public class Symbol {

    private final Namespace namespace;

    private final String name;

    public Symbol(Namespace namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return namespace + " [" + name + "]";
    }

}
