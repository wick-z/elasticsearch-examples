package luvtk.document;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author luvtk
 * @since 1.0.0
 * @see <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.8/java-docs-index.html">index api</a>
 * @see <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.8/java-admin-indices.html">admin indices api</a>
 */
public class IndexAPI {

    /**
     * transport client
     */
    private TransportClient client;
    /**
     * indices administration client
     */
    private IndicesAdminClient indicesAdminClient;
    /**
     * log using slf4j
     */
    private static final Logger LOG = LoggerFactory.getLogger(IndexAPI.class);

    /**
     * init
     *
     * @throws UnknownHostException
     */
    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", false).build();

        client = new PreBuiltTransportClient(settings).addTransportAddress(
                new TransportAddress(InetAddress.getByName("localhost"), 9300));

        indicesAdminClient = client.admin().indices();
    }

    /**
     * create an index with settings and mappings
     */
    @Test
    public void createIndexWithSettingsAndMappings() {
        String index = "book";
        String type = "novel";
        try {
            // add some settings for index "book"
            CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
                    .setSettings(Settings.builder()
                            // create 3 primary shards
                            .put("number_of_shards", 3)
                            // create a replica for every single of primary shard
                            .put("number_of_replicas", 1).build())
                    // define mappings
                    .addMapping(type, "name", "type=text", "publishDate", "type=date,format=yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis", "country", "type=keyword").get();
            if (response.isAcknowledged()) {
                LOG.info("index:{} and type:{} created successfully!", index, type);
            } else {
                LOG.error("failed to create index:{} and type:{}!", index, type);
            }
        } catch (Exception e) {
            LOG.error("failed to create index:{} and type:{}!", index, type, e);
        }
    }

    /**
     * create index only
     */
    @Test
    public void createIndexOnly() {
        String index = "people";
        try {
            // add some settings for index "people"
            CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
                    .setSettings(Settings.builder()
                            // create 3 primary shards
                            .put("number_of_shards", 3)
                            // create a replica for every single of primary shard
                            .put("number_of_replicas", 1).build()).get();
            if (response.isAcknowledged()) {
                LOG.info("index:{} created successfully!", index);
            } else {
                LOG.error("failed to create index:{}!", index);
            }
        } catch (Exception e) {
            LOG.error("failed to create index:{}!", index, e);
        }
    }

    /**
     * 为索引index增加别名
     */
    @Test
    public void addAlias2Index() {
        String index = "people";
        String alias = "people_alias1";
        try {
            IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = indicesAdminClient.prepareAliases();
            // here's to check if alias existed,
//            IndicesExistsResponse aliasesExistResponse = indicesAdminClient.prepareExists(alias).get();
//            if (aliasesExistResponse.isExists()) {
//                LOG.warn("alias exists, deleting...");
//                indicesAliasesRequestBuilder.removeAlias(index, alias).get();
//            }

            AcknowledgedResponse acknowledgedResponse = indicesAliasesRequestBuilder.addAlias(index, alias).get();
            if (acknowledgedResponse.isAcknowledged()) {
                LOG.info("alias={} index={} created successfully!", alias, index);
            } else {
                LOG.error("alias={} index={}  failed to create!", alias, index);
            }
        } catch (Exception e) {
            LOG.error("alias={} index={}  failed to create!", alias, index, e);
        }
    }

    /**
     * define mappings to an existed index
     */
    @Test
    public void createMappings2ExistedIndex() {
        String index = "people";
        try {
            AcknowledgedResponse response = indicesAdminClient.preparePutMapping(index)
                    .setType("student")
                    // here's to provide the type in the source document as well like {"type_name":{"properties":{}}}
                    .setSource("{\n" +
                            "  \"properties\": {\n" +
                            "    \"name\": {\n" +
                            "      \"type\": \"text\"\n" +
                            "    }\n," +
                            "    \"no\": {\n" +
                            "      \"type\": \"long\"\n" +
                            "    }\n," +
                            "    \"age\": {\n" +
                            "      \"type\": \"short\"\n" +
                            "    }\n" +
                            "  }\n" +
                            "}", XContentType.JSON)
                    .get();
            if (response.isAcknowledged()) {
                LOG.info("define mappings to index:{} created successfully!", index);
            } else {
                LOG.error("failed to define mappings to index:{}!", index);
            }
        } catch (Exception e) {
            LOG.error("failed to define mappings to index:{}!", index, e);
        }
    }

    /**
     * define mappings to an existed index
     */
    @Test
    @Deprecated
    public void updateMappings4ExistedIndex() {
        String index = "people";
        try {
            AcknowledgedResponse response = indicesAdminClient.preparePutMapping(index)
                    .setType("student").setType("employee")
                    // here's to provide the type in the source document as well like {"type_name":{"properties":{}}}
                    .setSource("{\n" +
                            " \"student\": {\n" +
                            "  \"properties\": {\n" +
                            "    \"name\": {\n" +
                            "      \"type\": \"text\"\n" +
                            "    }\n," +
                            "    \"no\": {\n" +
                            "      \"type\": \"long\"\n" +
                            "    }\n," +
                            "    \"age\": {\n" +
                            "      \"type\": \"short\"\n" +
                            "    }\n" +
                            "  }\n" +
                            " }\n," +
                            " \"employee\": {\n" +
                            "  \"properties\": {\n" +
                            "    \"name\": {\n" +
                            "      \"type\": \"text\"\n" +
                            "    }\n," +
                            "    \"dept\": {\n" +
                            "      \"type\": \"keyword\"\n" +
                            "    }\n," +
                            "    \"age\": {\n" +
                            "      \"type\": \"short\"\n" +
                            "    }\n" +
                            "  }\n" +
                            " }\n" +
                            "}", XContentType.JSON)
                    .get();
            if (response.isAcknowledged()) {
                LOG.info("update mappings for index:{} created successfully!", index);
            } else {
                LOG.error("failed to update mappings for index:{}!", index);
            }
        } catch (Exception e) {
            LOG.error("failed to update mappings for index:{}!", index, e);
        }
    }

    /**
     * get settings from indices
     */
    @Test
    public void getSettingsFromIndices() {
        String peopleIndexName = "people";
        String bookIndexName = "book";
        try {
            GetSettingsResponse response = indicesAdminClient.prepareGetSettings(peopleIndexName, bookIndexName).get();
            for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
                String index = cursor.key;
                Settings settings = cursor.value;
                Integer shards = settings.getAsInt("index.number_of_shards", null);
                Integer replicas = settings.getAsInt("index.number_of_replicas", null);
                LOG.info("get settings from index:{}! shards:{}, replicas:{}", index, shards, replicas);
            }
        } catch (Exception e) {
            LOG.info("failed to get settings from index!");
        }
    }

    /**
     * update settings for index
     */
    @Test
    public void updateSettings4Index() {
        String peopleIndexName = "people";
        String bookIndexName = "book";
        try {
            // here could be supported for multi indices on the same settings modifying
            AcknowledgedResponse response = indicesAdminClient.prepareUpdateSettings(peopleIndexName, bookIndexName)
                    .setSettings(Settings.builder()
                            .put("number_of_replicas", 1)
                    )
                    .get();
            if (response.isAcknowledged()) {
                LOG.info("update settings for index {} {} successfully!", peopleIndexName, bookIndexName);
            } else {
                LOG.error("failed to update settings for index {} {}", peopleIndexName, bookIndexName);
            }
        } catch (Exception e) {
            LOG.error("failed to update settings for index!");
        }
    }

    /**
     * refresh indices
     */
    @Test
    public void refreshIndices() {
        String peopleIndexName = "people";
        String bookIndexName = "book";
        try {
            // refresh 2 indices
            RefreshResponse response = indicesAdminClient.prepareRefresh(peopleIndexName, bookIndexName)
                    .get();
            if (response.getStatus().equals(RestStatus.OK)) {
                LOG.info("refresh indices {} {} successfully!", peopleIndexName, bookIndexName);
            } else {
                LOG.error("failed to refresh indices {} {} ", peopleIndexName, bookIndexName);
            }
        } catch (Exception e) {
            LOG.error("failed to refresh indices {} {} ", peopleIndexName, bookIndexName);
        }
    }

    @After
    public void destroy() {
        client.close();
    }
}
