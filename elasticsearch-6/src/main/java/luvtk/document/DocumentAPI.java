package luvtk.document;

import com.google.common.collect.Maps;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
    public void deleteDocsByQuery() {
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

    /**
     * update existed doc with <em>UpdateRequest</em>
     * <blockquote>
     *         The update API also support passing a partial document, which will be merged into the existing document
     *         (simple recursive merge, inner merging of objects, replacing core "keys/values" and arrays).
     * </blockquote>
     */
    @Test
    public void updateDocByRequest() {
        String index = "people";
        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index);
            // default type "_doc", equivalent to the type we created the mappings
            updateRequest.type("_doc");
            updateRequest.id("THFPbHEBT1iPBaUMEu8d");
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    // update existed field of doc
//                    .field("name", "luvtk")
                    // a field non-existed, update would be add the new field to the doc
                    .field("create_time", new Date())
                    .endObject());
            // send this request to client
            UpdateResponse updateResponse = client.update(updateRequest).get();
            LOG.info("update document of index={} type={}, id={}, result={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", updateResponse);
        } catch (Exception e) {
            LOG.error("failed to update document of index={} type={}, id={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", e);
        }
    }

    /**
     *  update doc by Request using script
     * <blockquote>
     *         The update API also support passing a partial document, which will be merged into the existing document
     *         (simple recursive merge, inner merging of objects, replacing core "keys/values" and arrays).
     * </blockquote>
     */
    @Test
    public void updateDocByRequestWithBuiltinScript() {
        String index = "people";
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, "_doc", "THFPbHEBT1iPBaUMEu8d")
                    .script(new Script("ctx._source.gender = \"male\""));
            UpdateResponse updateResponse = client.update(updateRequest).get();
            LOG.info("update document of index={} type={}, id={}, result={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", updateResponse);
        } catch (Exception e) {
            LOG.error("failed to update document of index={} type={}, id={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", e);
        }
    }

    /**
     * update doc with client
     * <blockquote>
     *         The update API also support passing a partial document, which will be merged into the existing document
     *         (simple recursive merge, inner merging of objects, replacing core "keys/values" and arrays).
     * </blockquote>
     */
    @Test
    public void updateDocByClient() {
        String index = "people";
        try {
            UpdateResponse updateResponse = client.prepareUpdate(index, "_doc", "THFPbHEBT1iPBaUMEu8d")
                    .setDoc(jsonBuilder()
                            .startObject()
//                            .field("age", 18)
                            // status is not defined in the mappings, update would add this field into the type
                            .field("status", "on")
                            .endObject())
                    .get();
            LOG.info("update document of index={} type={}, id={}, result={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", updateResponse);
        } catch (Exception e) {
            LOG.error("failed to update document of index={} type={}, id={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", e);
        }
    }

    /**
     * update doc by builtin script
     * <em>curl -X'POST' -H'Content-Type:application/json'
     * -d'{"script":{"lang":"painless","inline":"ctx._source.name = luvtk2"}}'
     * http://localhost:9200/people/_doc/THFPbHEBT1iPBaUMEu8d/_update</em>
     *
     */
    @Test
    public void updateDocByClientWithBuiltinScript() {
        String index = "people";
        // @NOTE type of value in params should be specified, as defined in mappings
        Map<String, Object> params = Maps.newHashMap();
        try {
            params.put("age", 10);
            UpdateResponse updateResponse = client.prepareUpdate(index, "_doc", "THFPbHEBT1iPBaUMEu8d")
                    // script type : inline, script lang: default lang, painless
                    // The id for this {@link Script} if the {@link ScriptType} is {@link ScriptType#STORED}.
                    // The code for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
                    // The user-defined params to be bound for script execution.
//                    .setScript(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "ctx._source.name = \"luvtk2\"", Collections.EMPTY_MAP))
                    .setScript(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "ctx._source.age += params.age", params))
                    .get();
            LOG.info("update document of index={} type={}, id={}, result={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", updateResponse);
        } catch (Exception e) {
            LOG.error("failed to update document of index={} type={}, id={}", index, "_doc", "THFPbHEBT1iPBaUMEu8d", e);
        }
    }

    /**
     * <blockquote>
     *     If the document does not exist, the content of the upsert element(index request) will be used to index the fresh doc
     *     If the document exists, then just update the field, if the field does not exist, the content of the update request will add to the type
     * </blockquote>
     */
    @Test
    public void upsert() {
        String index = "people";
        try {
            IndexRequest indexRequest = new IndexRequest(index, "_doc", "1")
                    .source(jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("gender", "female")
                            // add a new field
                            .field("update_status", "1")
                            .endObject());
            UpdateRequest updateRequest = new UpdateRequest(index, "_doc", "1")
                    .doc(jsonBuilder()
                            .startObject()
                            .field("gender", "male")
                            .endObject())
                    .upsert(indexRequest);
            UpdateResponse updateResponse = client.update(updateRequest).get();
            LOG.info("update document of index={} type={}, id={}, result={}", index, "_doc", 1, updateResponse);
        } catch (Exception e) {
            LOG.info("update document of index={} type={}, id={}", index, "_doc", 1, e);
        }
    }

    @After
    public void destroy() {
        client.close();
    }
}
