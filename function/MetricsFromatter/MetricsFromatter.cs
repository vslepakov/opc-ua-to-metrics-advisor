using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace MetricsFromatter
{
    public static class MetricsFromatter
    {
        [FunctionName("MetricsFromatter")]
        public static async Task Run([EventHubTrigger("opc-messages", Connection = "EventHubConnectionString", ConsumerGroup ="metrics")] EventData[] events, ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
            var containerName = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONTAINER_NAME");
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    var json = JObject.Parse(messageBody);

                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");

                    var sourceTimeStamp = DateTime.Parse((string)json["SourceTimestamp"]);
                    var fileName = $"{(string)json["PartitionKey"]}/{sourceTimeStamp:yyyy/MM/dd/HH/mm}.json";

                    var blobServiceClient = new BlobServiceClient(connectionString);
                    var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                    var blobClient = containerClient.GetBlobClient(fileName);

                    log.LogInformation($"Uploading metrics file: {fileName}");

                    var metric = $"[{{\"date\": \"{sourceTimeStamp:yyyy-MM-ddTHH:mm:00Z}\",\"{(string)json["DisplayName"]}\": {(double)json["Value"]}}}]";
                    using var stream = new MemoryStream(Encoding.UTF8.GetBytes(metric));
                    await blobClient.UploadAsync(stream, new BlobHttpHeaders { ContentType = "application/json" });
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
