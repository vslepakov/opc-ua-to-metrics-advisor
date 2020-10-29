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
using Newtonsoft.Json;

namespace MetricsFromatter
{
    public static class MetricsFromatter
    {
        [FunctionName("MetricsFromatter")]
        public static async Task Run([EventHubTrigger("opc-messages", Connection = "EventHubConnectionString", ConsumerGroup ="metrics")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    log.LogInformation($"Event Hub trigger function received a message: {messageBody}");

                    // Due to Metrics Advisor Limitations:
                    // Assumption / Pre-condition is that all relevant data points have the same timestamp
                    var opcUaData = JsonConvert.DeserializeObject<IList<OpcUaDataPoint>>(messageBody);

                    var appUriGroups = opcUaData.GroupBy(data => data.ApplicationUri);

                    if (appUriGroups.Any())
                    {
                        foreach(var group in appUriGroups)
                        {
                            var first = group.First();
                            var fileName = $"{first.ApplicationUri}/{first.SourceTimestamp:yyyy/MM/dd/HH/mm}.json";
                            var metrics = CreateOutputMetrics(group.ToList(), first.SourceTimestamp);

                            await UploadMetricsToBlobAsync(fileName, metrics, log);
                        }
                    }
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

        private static async Task UploadMetricsToBlobAsync(string fileName, string metrics, ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
            var containerName = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONTAINER_NAME");

            var blobClientOptions = new BlobClientOptions();
            blobClientOptions.Retry.MaxRetries = 10;
            var blobServiceClient = new BlobServiceClient(connectionString, blobClientOptions);
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            var blobClient = containerClient.GetBlobClient(fileName);

            log.LogInformation($"Uploading metrics file: {fileName}");

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(metrics));
            await blobClient.UploadAsync(stream, new BlobHttpHeaders { ContentType = "application/json" });
        }

        private static string CreateOutputMetrics(IList<OpcUaDataPoint> allData, DateTime timestamp)
        {
            var metrics = allData.Select(data => $"\"{data.DisplayName}\": {data.Value}");
            return $"[{{\"date\": \"{timestamp:yyyy-MM-ddTHH:mm:00Z}\", {string.Join(',', metrics)}}}]";
        }
    }
}
