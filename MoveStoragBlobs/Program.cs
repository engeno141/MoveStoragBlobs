using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace MoveStoragBlobs
{
    class Program
    {
        static void Main(string[] args)
        {

        // Execute this program from a Machine that has access to azure blob storage through private endpoint
        // This code is not intended for PRODUCTION, its just an exemple of moving blobs using flat listening
        // https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-list?tabs=dotnet#flat-listing-versus-hierarchical-listing
        // https://markheath.net/post/azure-blob-copy-quick


            var connectionString = "";
            var blobServiceClient = new Azure.Storage.Blobs.BlobContainerClient(connectionString, "source");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();                     

            var sourceContainer = blobClient.GetContainerReference("source");
            var destContainer = blobClient.GetContainerReference("dest");

            var sourcePaths = ListBlobsFlatListing(blobServiceClient).GetAwaiter().GetResult();

            foreach (var sourcePath in sourcePaths)
            {
                var sourceBlob = sourceContainer.GetBlockBlobReference(sourcePath);
                var destinationPath = Path.GetFileName(sourcePath);                                      // rename if needed using your own logic and state
                var destBlob = destContainer.GetBlockBlobReference(destinationPath);
                destBlob.StartCopyAsync(sourceBlob).GetAwaiter().GetResult();
                // sourceBlob.Delete();                                                                  // Delete if needed
            }
        }
       
        private static async Task<List<string>> ListBlobsFlatListing(BlobContainerClient blobContainerClient)
        {          
            var paths = new List<string>();

            var blobs = blobContainerClient.GetBlobsAsync().AsPages();

            // Enumerate the blobs returned for each page.
            await foreach (Azure.Page<BlobItem> blobPage in blobs)
            {
                foreach (BlobItem blobItem in blobPage.Values)
                {
                    paths.Add(blobItem.Name);
                }
            }

            return paths;
        }      
    }
}