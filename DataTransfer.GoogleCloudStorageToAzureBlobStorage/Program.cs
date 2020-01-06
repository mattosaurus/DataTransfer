using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

namespace DataTransfer.GoogleCloudStorageToAzureBlobStorage
{
    class Program
    {
        private const string _schema = "dbo";
        private const string _table = "<YOURTABLENAME>";
        private const string _sqlConnectionString = "<YOURSQLCONNECTIONSTRING>";
        private const string _cloudStorageBucketName = "<BUCKETNAMEWHERESTORAGEOBJECTSEXIST>";
        private const string _cloudStorageObjectPrefix = "<PREFIXOFYOURSTORAGEOBJECTS>";
        private const string _googleCredentialPath = "<PATHTOYOURGOOGLECREDENTIALS>";
        private const string _azureBlobStorageContainer = "<YOURBLOBSTORAGECONTAINER>";
        private const string _azureStorageConnectionString = "<YOURAZUREBLOBSTORAGECREDENTIALS>";

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
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_azureStorageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(_azureBlobStorageContainer);

            await foreach (Google.Apis.Storage.v1.Data.Object storageObject in GetStorageObjects(storageClient, _cloudStorageBucketName, _cloudStorageObjectPrefix))
            {
                using (Stream stream = await DownloadStreamFromStorageAsync(storageClient, storageObject))
                {
                    CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(storageObject.Name.Replace(".gzip", ""));
                    using (Stream decompressedStream = DecompressStreamToStream(stream))
                    {
                        await blockBlob.UploadFromStreamAsync(decompressedStream);
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

        public static Stream DecompressStreamToStream(Stream stream)
        {
            return new GZipStream(stream, CompressionMode.Decompress);
        }
    }
}
