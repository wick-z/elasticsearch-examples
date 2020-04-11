package luvtk.document;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author luvtk
 * @since 1.0.0
 * @see <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.8/java-docs-index.html">index api</a>
 */
public class DocumentAPI {

    /**
     * transport client
     */
    private TransportClient client;

    /**
     * log using slf4j
     */
    private static final Logger LOG = LoggerFactory.getLogger(DocumentAPI.class);

    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", false).build();

        client = new PreBuiltTransportClient(settings).addTransportAddress(
                new TransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    /**
     * several ways to make a doc format:
     * <ul>
     *   <li>Manually (aka do it yourself) using native byte[] or as a String</li>
     *   <li>Using a Map that will be automatically converted to its JSON equivalent</li>
     *   <li>Using a third party library to serialize your beans such as Jackson</li>
     *   <li>Using built-in helpers XContentFactory.jsonBuilder()</li>
     * </ul>
     */
    @Test
    public void addDocToIndex() {
        String index = "people";
        String type = "student";
        try {
            // if we don't specify the id here, es would use its own id generator to generate id
            // here we could use the dummy type "_doc" or type "student"
            IndexResponse response = client.prepareIndex(index, "_doc")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("name", "joseph")
                            .field("age", 19)
                            .field("no", 20335L)
                            .endObject()
                    )
                    .get();
            if (response.status().equals(RestStatus.CREATED)) {
                LOG.info("add doc to index {} type {} successfully, result:{}", index, type, response);
            } else {
                LOG.error("failed to add doc to index{} type{}, result:{}", index, type, response);
            }
        } catch (IOException e) {
            LOG.error("failed to add doc to index{} type{}", index, type, e);
        }
    }

    /**
     * GET API：get source from index
     */
    @Test
    public void getSourceFromIndex() {
        String index = "people";
        String type = "student";
        String id = "Q3GQTXEBT1iPBaUMGu_B";
        try {
            GetResponse response = client.prepareGet(index, type, id).get();
            LOG.info("get source from index={} type={}, id={}, result={}", index, type, id, response);
        } catch (Exception e) {
            LOG.error("failed to get source from index={} type={} id={}", index, type, id, e);
        }
    }

    /**
     * DELETE API: delete source from index
     */
    @Test
    public void deleteSourceFromIndex() {
        String index = "people";
        String type = "student";
        String id = "Q3GQTXEBT1iPBaUMGu_B";
        try {
            DeleteResponse response = client.prepareDelete(index, type, id).get();
            LOG.info("delete source from index={} type={}, id={}, result={}", index, type, id, response.getResult());
        } catch (Exception e) {
            LOG.error("failed to delete source from index={} type={} id={}", index, type, id, e);
        }
    }

    /**
     * <blockquote>The delete by query API allows one to delete a given set of documents based on the result of a query</blockquote>
     * delete result=BulkByScrollResponse[took=337.4ms,timed_out=false,sliceId=null,updated=0,created=0,deleted=1,batches=1,versionConflicts=0
     * ,noops=0,retries=0,throttledUntil=0s,bulk_failures=[],search_failures=[]]
     */
    @Test
    public void deleteByQuery() {
        try {
            BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                    .filter(QueryBuilders.matchQuery("name", "joseph"))
                    // specify indices
                    .source("people")
                    .get();
            LOG.info("delete result={}", response);
        } catch (Exception e) {
            LOG.error("failed to delete by query", e);
        }
    }

    @After
    public void destroy() {
        client.close();
    }
}
