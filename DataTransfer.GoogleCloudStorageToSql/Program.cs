using CsvHelper;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Google.Cloud.Storage.V1;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace DataTransfer.GoogleCloudStorageToSql
{
    class Program
    {
        private const string _cloudStorageBucketName = "<BUCKETNAMEWHERESTORAGEOBJECTSEXIST>";
        private const string _cloudStorageObjectPrefix = "<PREFIXOFYOURSTORAGEOBJECTS>";
        private const string _googleCredentialPath = "<YOURTABLENAME>";
        private const string _sqlSchema = "dbo";
        private const string _sqlTable = "<YOURTABLENAME>";
        private const string _sqlConnectionString = "<YOURSQLCONNECTIONSTRING>";

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
            StorageClient storageClient = StorageClient.Create(googleCredential);
            DataTable schemaDataTable = GetDataTableSchema(_sqlSchema, _sqlTable);

            await foreach(Google.Apis.Storage.v1.Data.Object storageObject in GetStorageObjects(storageClient, _cloudStorageBucketName, _cloudStorageObjectPrefix))
            {
                using (Stream storageObjectStream = await DownloadStreamFromStorageAsync(storageClient, storageObject))
                using (GZipStream gZipStream = new GZipStream(storageObjectStream, CompressionMode.Decompress))
                using (StreamReader streamReader = new StreamReader(gZipStream))
                using (CsvReader csvReader = new CsvReader(streamReader))
                {
                    // Do any configuration to CsvReader before creating CsvDataReader.
                    csvReader.Configuration.HasHeaderRecord = true;
                    csvReader.Configuration.TypeConverterOptionsCache.GetOptions<string>().NullValues.Add("");

                    using (CsvDataReader csvDataReader = new CsvDataReader(csvReader, schemaDataTable))
                    {
                        using (SqlConnection connection = new SqlConnection(_sqlConnectionString))
                        {
                            await connection.OpenAsync();

                            using (SqlBulkCopy bulk = new SqlBulkCopy(connection))
                            {
                                bulk.DestinationTableName = $"{_sqlSchema}.{_sqlTable}";

                                await bulk.WriteToServerAsync(csvDataReader);
                            }
                        }
                    }
                }
            }
        }

        public static async IAsyncEnumerable<Google.Apis.Storage.v1.Data.Object> GetStorageObjects(StorageClient storageClient, string bucketName, string prefix)
        {
            var result = storageClient.ListObjectsAsync(bucketName, prefix).GetEnumerator();

            while (await result.MoveNext(CancellationToken.None))
            {
                yield return result.Current;
            }
        }

        public static async Task<Stream> DownloadStreamFromStorageAsync(StorageClient storageClient, Google.Apis.Storage.v1.Data.Object storageObject)
        {
            MemoryStream memoryStream = new MemoryStream();
            await storageClient.DownloadObjectAsync(storageObject, memoryStream);
            memoryStream.Position = 0;
            return memoryStream;
        }

        public static DataTable GetDataTableSchema(string schema, string tableName)
        {
            DataTable dataTable = new DataTable();

            using (SqlConnection _sqlConnection = new SqlConnection(_sqlConnectionString))
            using (SqlCommand command = new SqlCommand("select top 0 * from " + schema + "." + tableName))
            {
                command.Connection = _sqlConnection;
                _sqlConnection.Open();

                using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    dataTable = reader.GetSchemaTable();
                }

                _sqlConnection.Close();
            }

            return dataTable;
        }
    }
}
