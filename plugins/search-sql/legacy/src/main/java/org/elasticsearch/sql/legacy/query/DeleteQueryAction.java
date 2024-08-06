


package org.elasticsearch.sql.legacy.query;


import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.sql.legacy.domain.Delete;
import org.elasticsearch.sql.legacy.domain.Where;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.query.maker.QueryMaker;

public class DeleteQueryAction extends QueryAction {

    private final Delete delete;
    private DeleteByQueryRequestBuilder request;

    public DeleteQueryAction(Client client, Delete delete) {
        super(client, delete);
        this.delete = delete;
    }

    @Override
    public SqlElasticDeleteByQueryRequestBuilder explain() throws SqlParseException {
        this.request = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);

        setIndicesAndTypes();
        setWhere(delete.getWhere());
        SqlElasticDeleteByQueryRequestBuilder deleteByQueryRequestBuilder =
                new SqlElasticDeleteByQueryRequestBuilder(request);
        return deleteByQueryRequestBuilder;
    }


    /**
     * Set indices and types to the delete by query request.
     */
    private void setIndicesAndTypes() {

        DeleteByQueryRequest innerRequest = request.request();
        innerRequest.indices(query.getIndexArr());
        String[] typeArr = query.getTypeArr();
        if (typeArr != null) {
            innerRequest.getSearchRequest().types(typeArr);
        }
//        String[] typeArr = query.getTypeArr();
//        if (typeArr != null) {
//            request.set(typeArr);
//        }
    }


    /**
     * Create filters based on
     * the Where clause.
     *
     * @param where the 'WHERE' part of the SQL query.
     * @throws SqlParseException
     */
    private void setWhere(Where where) throws SqlParseException {
        if (where != null) {
            QueryBuilder whereQuery = QueryMaker.explain(where);
            request.filter(whereQuery);
        } else {
            request.filter(QueryBuilders.matchAllQuery());
        }
    }

}
