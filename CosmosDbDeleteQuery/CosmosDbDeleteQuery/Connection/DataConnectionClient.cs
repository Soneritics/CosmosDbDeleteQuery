using System;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosDbDeleteQuery.Connection
{
    /// <summary>
    /// Data client abstraction.
    /// </summary>
    public class DataConnectionClient
    {
        public DocumentClient Client { get; }
        public string DatabaseId { get; }
        public string CollectionId { get; }
        public bool EnableCrossPartitionQuery { get; }
        public PartitionKey PartitionKey { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataConnectionClient"/> class.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="key">The key.</param>
        /// <param name="databaseId">The database identifier.</param>
        /// <param name="collectionId">The collection identifier.</param>
        /// <param name="enableCrossPartitionQuery">if set to <c>true</c> [enable cross partition query].</param>
        /// <param name="partitionKey">The partition key.</param>
        public DataConnectionClient(string endpoint, string key, string databaseId, string collectionId, bool enableCrossPartitionQuery, string partitionKey = "id")
        {
            DatabaseId = databaseId;
            CollectionId = collectionId;
            EnableCrossPartitionQuery = enableCrossPartitionQuery;
            PartitionKey = new PartitionKey(partitionKey);
            Client = new DocumentClient(
                serviceEndpoint: new Uri(endpoint),
                authKeyOrResourceToken: key,
                connectionPolicy: GetConnectionPolicy()
            );
        }

        /// <summary>
        /// Gets the connection policy.
        /// For this application, only reads should be allowed which need the data directly.
        /// Therefore, the policy is very strict. If the connection is unavailable, we don't
        /// want the user to wait endlessly, but rather show an error that the database is down.
        /// </summary>
        /// <returns></returns>
        protected ConnectionPolicy GetConnectionPolicy()
        {
            return new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                RequestTimeout = new TimeSpan(0, 0, 5),
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 3,
                    MaxRetryWaitTimeInSeconds = 1
                }
            };
        }
    }
}
