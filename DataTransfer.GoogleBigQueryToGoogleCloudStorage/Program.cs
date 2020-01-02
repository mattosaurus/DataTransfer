using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using System.Threading.Tasks;

namespace DataTransfer.GoogleBigQueryToGoogleCloudStorage
{
    class Program
    {
        private const string _bigQueryProjectId = "<BIGQUERYPROJECTID>";
        private const string _bigQueryDataSetId = "<BIGQUERYDATASETID>";
        private const string _bigQueryTableId = "<BIGQUERYTABLEID>";
        private const string _cloudStorageDestinationUri = "<URIOFCLOUDSTORAGEOBJECT>";
        private const string _googleCredentialPath = "<YOURTABLENAME>";

        static int Main(string[] args)
        {
            try
            {
                // Start!
                MainAsync(args).Wait();
                return 0;
            }
            catch
            {
                return 1;
            }
        }

        static async Task MainAsync(string[] args)
        {
            GoogleCredential googleCredential = GoogleCredential.FromFile(_googleCredentialPath);
            BigQueryClient bigQueryClient = await BigQueryClient.CreateAsync(_bigQueryProjectId, googleCredential);
            string query = $"select * from `{_bigQueryProjectId}.{_bigQueryDataSetId}.{_bigQueryTableId}`";

            BigQueryResults results = await RunBigQueryAsync(bigQueryClient, query);

            await ExportBigQueryTableToStorageAsync(bigQueryClient, _cloudStorageDestinationUri, results);
        }

        public static async Task<BigQueryResults> RunBigQueryAsync(BigQueryClient bigQueryClient, string query)
        {
            BigQueryJob bigQueryJob = await bigQueryClient.CreateQueryJobAsync(
                sql: query,
                parameters: null
                );

            await bigQueryJob.PollUntilCompletedAsync();

            return await bigQueryClient.GetQueryResultsAsync(bigQueryJob.Reference);
        }

        public static async Task ExportBigQueryTableToStorageAsync(BigQueryClient bigQueryClient, string destinationUri, BigQueryResults results)
        {
            CreateExtractJobOptions jobOptions = new CreateExtractJobOptions()
            {
                DestinationFormat = FileFormat.Csv,
                Compression = CompressionType.Gzip
            };

            BigQueryJob job = bigQueryClient.CreateExtractJob(
                projectId: results.TableReference.ProjectId,
                datasetId: results.TableReference.DatasetId,
                tableId: results.TableReference.TableId,
                destinationUri: destinationUri,
                options: jobOptions
            );

            await job.PollUntilCompletedAsync();
        }
    }
}
