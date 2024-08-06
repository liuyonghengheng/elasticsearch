package com.infinilabs.security.test.helper.rest;

import com.infinilabs.security.test.helper.cluster.ClusterInfo;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.*;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class EasyRestHelper {
    protected final Logger log = LogManager.getLogger(EasyRestHelper.class);

    public boolean enableHTTPClientSSL = true;
    public boolean enableHTTPClientSSLv3Only = false;
    public boolean sendAdminCertificate = false;
    public boolean trustHTTPServerCertificate = true;
    public boolean sendHTTPClientCredentials = true;
    public String keystore = "node-0-keystore.jks";
    private String httpHost;
    private int httpPort;
    public  String prefix;
    private  String user;
    private  String password;
    private ClusterInfo clusterInfo;



    public EasyRestHelper(String httpHost, int httpPort, String prefix) {
        this.httpHost = httpHost;
        this.httpPort = httpPort;
        this.prefix = prefix;
    }
    public EasyRestHelper(String httpHost, int httpPort, String user, String password) {
        this.httpHost = httpHost;
        this.httpPort = httpPort;
        this.user = user;
        this.password = password;
    }

    public EasyRestHelper(ClusterInfo clusterInfo, String user, String password) {
        this.user = user;
        this.password = password;
        this.clusterInfo = clusterInfo;
    }


    public String executeSimpleRequest(final String request) throws Exception {

        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        try {
            httpClient = getHTTPClient();
            response = httpClient.execute(new HttpGet(getHttpServerUri() + "/" + request));

            if (response.getStatusLine().getStatusCode() >= 300) {
                throw new Exception("Statuscode " + response.getStatusLine().getStatusCode());
            }

            return IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
        } finally {

            if (response != null) {
                response.close();
            }

            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public HttpResponse executeGetRequest(final String request, Header... header) throws Exception {
        return executeRequest(new HttpGet(getHttpServerUri() + "/" + request), header);
    }

    public HttpResponse executeHeadRequest(final String request, Header... header) throws Exception {
        return executeRequest(new HttpHead(getHttpServerUri() + "/" + request), header);
    }

    public HttpResponse executeOptionsRequest(final String request) throws Exception {
        return executeRequest(new HttpOptions(getHttpServerUri() + "/" + request));
    }

    public HttpResponse executePutRequest(final String request, String body, Header... header) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + request);
        if (body != null && !body.isEmpty()) {
            uriRequest.setEntity(new StringEntity(body));
        }
        return executeRequest(uriRequest, header);
    }

    public HttpResponse executeDeleteRequest(final String request, Header... header) throws Exception {
        return executeRequest(new HttpDelete(getHttpServerUri() + "/" + request), header);
    }

    public HttpResponse executePostRequest(final String request, String body, Header... header) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + request);
        if (body != null && !body.isEmpty()) {
            uriRequest.setEntity(new StringEntity(body));
        }

        return executeRequest(uriRequest, header);
    }

    public HttpResponse executePatchRequest(final String request, String body, Header... header) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + request);
        if (body != null && !body.isEmpty()) {
            uriRequest.setEntity(new StringEntity(body));
        }
        return executeRequest(uriRequest, header);
    }

    public HttpResponse executeRequest(HttpUriRequest uriRequest, Header... header) throws Exception {

        CloseableHttpClient httpClient = null;
        try {

            httpClient = getHTTPClient();

            if (header != null && header.length > 0) {
                for (int i = 0; i < header.length; i++) {
                    Header h = header[i];
                    uriRequest.addHeader(h);
                }
            }

            if (!uriRequest.containsHeader("Content-Type")) {
                uriRequest.addHeader("Content-Type","application/json");
            }
            HttpResponse res = new HttpResponse(httpClient.execute(uriRequest));
            log.debug(res.getBody());
            return res;
        } finally {

            if (httpClient != null) {
                httpClient.close();
            }
        }
    }


    protected final String getHttpServerUri() {
        final String address = "http" + "s" + "://" + clusterInfo.httpHost + ":" + clusterInfo.httpPort;
        log.info("Connect to {}", address);
        return address;
    }

    protected final CloseableHttpClient getHTTPClient() throws Exception {

        final HttpClientBuilder hcb = HttpClients.custom();

        if (sendHTTPClientCredentials) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(user,  password);
            provider.setCredentials(AuthScope.ANY, credentials);
            hcb.setDefaultCredentialsProvider(provider);

            SSLContext sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (chain, authType) -> true).build();
            hcb.setSSLContext(sslContext).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);

        }

//        if (enableHTTPClientSSL) {
//
//            log.debug("Configure HTTP client with SSL");
//
//            final SSLContextBuilder sslContextbBuilder = SSLContexts.custom();
//
//            final SSLContext sslContext = sslContextbBuilder.build();
//
//            String[] protocols = null;
//
//            if (enableHTTPClientSSLv3Only) {
//                protocols = new String[] { "SSLv3" };
//            } else {
//                protocols = new String[] { "TLSv1", "TLSv1.1", "TLSv1.2" };
//            }
//
//            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
//                sslContext,
//                protocols,
//                null,
//                NoopHostnameVerifier.INSTANCE);
//
//            hcb.setSSLSocketFactory(sslsf);
//        }

        hcb.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(60 * 1000).build());

        return hcb.build();
    }

    public class HttpResponse {
        private final CloseableHttpResponse inner;
        private final String body;
        private final Header[] header;
        private final int statusCode;
        private final String statusReason;

        public HttpResponse(CloseableHttpResponse inner) throws IllegalStateException, IOException {
            super();
            this.inner = inner;
            final HttpEntity entity = inner.getEntity();
            if(entity == null) { //head request does not have a entity
                this.body = "";
            } else {
                this.body = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
            }
            this.header = inner.getAllHeaders();
            this.statusCode = inner.getStatusLine().getStatusCode();
            this.statusReason = inner.getStatusLine().getReasonPhrase();
            inner.close();
        }

        public String getContentType() {
            Header h = getInner().getFirstHeader("content-type");
            if(h!= null) {
                return h.getValue();
            }
            return null;
        }

        public boolean isJsonContentType() {
            String ct = getContentType();
            if(ct == null) {
                return false;
            }
            return ct.contains("application/json");
        }

        public CloseableHttpResponse getInner() {
            return inner;
        }

        public String getBody() {
            return body;
        }

        public Header[] getHeader() {
            return header;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getStatusReason() {
            return statusReason;
        }

        public List<Header> getHeaders() {
            return header==null? Collections.emptyList(): Arrays.asList(header);
        }

        @Override
        public String toString() {
            return "HttpResponse [inner=" + inner + ", body=" + body + ", header=" + Arrays.toString(header) + ", statusCode=" + statusCode
                + ", statusReason=" + statusReason + "]";
        }

    }
}
