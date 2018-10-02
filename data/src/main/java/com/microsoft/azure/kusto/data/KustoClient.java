package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.json.JSONException;
import org.json.JSONObject;

public class KustoClient {

    private final String adminCommandsPrefix = ".";
    private final String apiVersion = "v1";
    private final String defaultDatabaseName = "NetDefaultDb";

    private AadAuthenticationHelper aadAuthenticationHelper;
    private String clusterUrl;

    public KustoClient(KustoConnectionStringBuilder kcsb) {
        clusterUrl = kcsb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(kcsb);
    }

    public KustoResults execute(String command) throws DataServiceException, DataClientException {
        return execute(defaultDatabaseName, command);
    }

    public KustoResults execute(String database, String command) throws DataServiceException, DataClientException {
        String clusterEndpoint;
        if (command.startsWith(adminCommandsPrefix)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, apiVersion);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, apiVersion);
        }
        return execute(database, command, clusterEndpoint);
    }

    private KustoResults execute(String database, String command, String clusterEndpoint) throws DataServiceException, DataClientException {
        String aadAccessToken = aadAuthenticationHelper.acquireAccessToken();
        String jsonString;
        try {
            jsonString = new JSONObject()
                    .put("db", database)
                    .put("csl", command).toString();
        } catch (JSONException e) {
            throw new DataClientException(clusterEndpoint, String.format(clusterEndpoint, "Error in executing command: %s, in database: %s", command, database), e);
        }

        return Utils.post(clusterEndpoint, aadAccessToken, jsonString);
    }
}