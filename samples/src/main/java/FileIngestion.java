import com.microsoft.azure.kusto.data.connection.DataConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

public class FileIngestion {

    private static final String appId = "<application Id aka service principal>";
    private static final String appKey = "<application key / secret>";
    private static final String appTenant = "<application tenant id or domain name>";

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-<cluster-name>.kusto.windows.net";
            String dbName = "<databaseName>";
            String tableName = "<tableName>";
            String dataMappingName = "<dataMappingName>";
            String filePath = "<localFilePath>";

            DataConnectionStringBuilder dcsb = DataConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath, appId, appKey, appTenant);
            IngestClient client = IngestClientFactory.createClient(dcsb);

            IngestionProperties ingestionProperties = new IngestionProperties(dbName, tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            FileSourceInfo fileSourceInfo = new FileSourceInfo(filePath, 0);
            IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}