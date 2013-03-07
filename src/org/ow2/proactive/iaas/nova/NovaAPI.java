package org.ow2.proactive.iaas.nova;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.AuthenticationException;

import net.minidev.json.JSONObject;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.ow2.proactive.iaas.IaasApi;
import org.ow2.proactive.iaas.IaasApiFactory;
import org.ow2.proactive.iaas.IaasInstance;

import com.jayway.jsonpath.JsonPath;


public class NovaAPI implements IaasApi {

    private static final Logger logger = Logger.getLogger(NovaAPI.class);

    private static Map<Integer, NovaAPI> instances;

    private long created;
    private HttpClient httpClient;
    private URI endpoint;
    private String sessionId;
    private String novaUri;

    // ////////////////////////
    // NOVA FACTORY
    // ////////////////////////

    public static IaasApi getNovaAPI(Map<String, String> args) throws URISyntaxException,
            AuthenticationException {
        return getNovaAPI(args.get(NovaAPIConstants.ApiParameters.USER_NAME),
                args.get(NovaAPIConstants.ApiParameters.PASSWORD),
                args.get(NovaAPIConstants.ApiParameters.TENANT_NAME),
                new URI(args.get(NovaAPIConstants.ApiParameters.API_URL)));
    }

    public static synchronized NovaAPI getNovaAPI(String username, String password, String tenantName,
            URI endpoint) throws AuthenticationException {
        if (instances == null) {
            instances = new HashMap<Integer, NovaAPI>();
        }
        int hash = (username + password + tenantName).hashCode();
        NovaAPI instance = instances.get(hash);
        if (instance == null || !isValid(instance.created)) {
            try {
                instances.remove(hash);
                instance = new NovaAPI(username, password, tenantName, endpoint);
            } catch (Throwable t) {
                throw new AuthenticationException("Failed to authenticate to " + endpoint, t);
            }
            instances.put(hash, instance);
        }
        return instance;
    }

    /**
     * SessionId provided by OpenStack are valid for 24 hours. So we have to
     * check is the cached one is still valid.
     * 
     * @param created
     * @return
     */
    private static boolean isValid(long created) {
        final int ALMOST_ONE_DAY = 1000 * (3600 * 24 - 10); // in ms, 1 day minus 10 seconds

        return (System.currentTimeMillis() - created < ALMOST_ONE_DAY);
    }

    private NovaAPI(String username, String password, String tenantName, URI endpoint) throws IOException {
        this.created = System.currentTimeMillis();
        this.endpoint = endpoint;
        this.httpClient = new DefaultHttpClient();
        authenticate(username, password, tenantName);
    }

    // ////////////////////////
    // NOVA COMMANDS
    // ////////////////////////

    private void authenticate(String username, String password, String tenant) throws IOException {
        // Retrieve a token id to list tenants
        JSONObject jsonCreds = new JSONObject();
        jsonCreds.put(NovaAPIConstants.ApiParameters.USER_NAME, username);
        jsonCreds.put(NovaAPIConstants.ApiParameters.PASSWORD, password);
        JSONObject jsonAuth = new JSONObject();
        jsonAuth.put(NovaAPIConstants.ApiParameters.PASSWORD_CREDENTIALS, jsonCreds);
        jsonAuth.put(NovaAPIConstants.ApiParameters.TENANT_NAME, tenant);
        JSONObject jsonReq = new JSONObject();
        jsonReq.put(NovaAPIConstants.ApiParameters.AUTH, jsonAuth);

        // Here we cannot use post() yet because sessionId is not set
        HttpPost post = new HttpPost(endpoint + "/tokens");
        post.addHeader("Content-type", "application/json");
        post.setEntity(new StringEntity(jsonReq.toString(), "UTF-8"));

        HttpResponse response = httpClient.execute(post);
        String entity = EntityUtils.toString(response.getEntity());

        logger.debug(entity);

        // Retrieve useful information from this response
        sessionId = JsonPath.read(entity, "$.access.token.id");
        try {
            novaUri = JsonPath.read(entity,
                    "$.access.serviceCatalog[?(@.type=='compute')].endpoints[0].publicURL");
            logger.info("Compute url is " + novaUri);
        } catch (RuntimeException ex) {
            throw new RuntimeException("Cannot parse service catalog - check your tenant name");
        }
    }

    public void listAvailableImages() throws ClientProtocolException, IOException {

        //JSONObject jReq = new JSONObject();
        //jReq.put("server", jServer);
        HttpResponse response = get("/servers/detail");
        String entity = EntityUtils.toString(response.getEntity());

        logger.debug(entity);
        response = get("/flavors/detail");
        entity = EntityUtils.toString(response.getEntity());

        logger.debug(entity);
    }

    public String createServer(String name, String imageRef, String flavorRef, String userData,
            Map<String, String> metaData) throws ClientProtocolException, IOException {

        JSONObject jsonMetaData = new JSONObject();
        jsonMetaData.putAll(metaData);

        JSONObject jServer = new JSONObject();
        jServer.put(NovaAPIConstants.InstanceParameters.NAME, name);
        jServer.put(NovaAPIConstants.InstanceParameters.IMAGE_REF, imageRef);
        jServer.put(NovaAPIConstants.InstanceParameters.FLAVOR_REF, flavorRef);
        jServer.put(NovaAPIConstants.InstanceParameters.META_DATA, jsonMetaData);
        jServer.put(NovaAPIConstants.InstanceParameters.USER_DATA, userData);

        JSONObject jReq = new JSONObject();
        jReq.put("server", jServer);

        logger.debug(jReq.toJSONString());

        HttpResponse response = post("/servers", jReq);
        String entity = EntityUtils.toString(response.getEntity());
        logger.debug(entity);

        String serverId = JsonPath.read(entity, "$.server.id");
        return serverId;
    }

    public boolean rebootServer(String serverId, String method) throws ClientProtocolException, IOException {
        JSONObject jReboot = new JSONObject();
        jReboot.put("type", method.toUpperCase());
        JSONObject jReq = new JSONObject();
        jReq.put("reboot", jReboot);

        HttpResponse response = post("/servers/" + serverId + "/action", jReq);
        response.getEntity().consumeContent();
        return response.getStatusLine().getStatusCode() == 202;
    }

    public boolean deleteServer(String serverId) throws ClientProtocolException, IOException {
        HttpResponse response = delete("/servers/" + serverId);
        logger.debug(response.getEntity());
        return response.getStatusLine().getStatusCode() == 204;
    }

    public String filter(String ressources, String criteria) throws ClientProtocolException, IOException {
        HttpResponse response = get("/" + ressources);
        String entity = EntityUtils.toString(response.getEntity());
        return JsonPath.read(entity, criteria).toString();
    }

    // ////////////////////////
    // GENERATE HTTP REQUESTS
    // ////////////////////////

    private HttpResponse post(String path, JSONObject content) throws ClientProtocolException, IOException {
        HttpPost post = new HttpPost(novaUri + path);
        post.addHeader("X-Auth-Token", sessionId);
        post.addHeader("Content-type", "application/json");
        post.setEntity(new StringEntity(content.toString(), "UTF-8"));

        return httpClient.execute(post);
    }

    private HttpResponse get(String path) throws ClientProtocolException, IOException {
        HttpGet get = new HttpGet(novaUri + path);
        get.addHeader("X-Auth-Token", sessionId);

        return httpClient.execute(get);
    }

    private HttpResponse delete(String path) throws ClientProtocolException, IOException {
        HttpDelete del = new HttpDelete(novaUri + path);
        del.addHeader("X-Auth-Token", sessionId);

        return httpClient.execute(del);
    }

    @Override
    public IaasInstance startInstance(Map<String, String> arguments) throws Exception {

        Map<String, String> metaData = Collections.emptyMap();

        String userData = "";
        if (arguments.containsKey(NovaAPIConstants.InstanceParameters.USER_DATA)) {
            userData = arguments.get(NovaAPIConstants.InstanceParameters.USER_DATA);
            userData = new String(Base64.encodeBase64(userData.getBytes()));
        }

        return new IaasInstance(createServer(arguments.get(NovaAPIConstants.InstanceParameters.NAME),
                arguments.get(NovaAPIConstants.InstanceParameters.IMAGE_REF),
                arguments.get(NovaAPIConstants.InstanceParameters.FLAVOR_REF), userData, metaData));
    }

    @Override
    public void stopInstance(IaasInstance instance) throws Exception {
        deleteServer(instance.getInstanceId());
    }

    @Override
    public boolean isInstanceStarted(IaasInstance instance) throws Exception {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getName() {
        return IaasApiFactory.IaasProvider.NOVA.name();
    }

    public class NovaAPIConstants {
        public class ApiParameters {
            static final String API_URL = "apiurl";
            static final String AUTH = "auth";
            static final String USER_NAME = "username";
            static final String PASSWORD = "password";
            static final String PASSWORD_CREDENTIALS = "passwordCredentials";
            static final String TENANT_NAME = "tenantName";
        }

        public class InstanceParameters {
            public static final String NAME = "name";
            public static final String TENANT_NAME = "";
            public static final String IMAGE_REF = "imageRef";
            public static final String FLAVOR_REF = "flavorRef";
            public static final String META_DATA = "metadata";
            public static final String USER_DATA = "user_data";
        }
    }

}
