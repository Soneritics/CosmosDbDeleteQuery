using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosDbDeleteQuery.Connection
{
    /// <summary>
    /// Data operations.
    /// </summary>
    public class DataClient
    {
        /// <summary>
        /// ID DTO to cast results of the Cosmos query
        /// </summary>
        private class IdPartitionKeyDto
        {
            public string Id { get; set; }
            public string PartitionKey { get; set; }
        }

        /// <summary>
        /// The client
        /// </summary>
        private readonly DataConnectionClient _client;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataClient"/> class.
        /// </summary>
        /// <param name="client">The client.</param>
        public DataClient(DataConnectionClient client)
        {
            _client = client;
        }

        /// <summary>
        /// Gets the count of a query. When an exception occurs, returns -1
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        /// <param name="retriesAllowed">The retries allowed.</param>
        /// <returns></returns>
        public int GetCount(string whereClause, int retriesAllowed = 10)
        {
            var query = $"SELECT VALUE COUNT(1) FROM c WHERE {whereClause}";
            var uri = UriFactory.CreateDocumentCollectionUri(_client.DatabaseId, _client.CollectionId);
            var feedOptions = _client.EnableCrossPartitionQuery ? new FeedOptions {EnableCrossPartitionQuery = true} : null;

            try
            {
                var result = _client.Client.CreateDocumentQuery(uri, query, feedOptions).AsEnumerable().FirstOrDefault();
                return (int) result;
            }
            catch
            {
                if (retriesAllowed <= 0)
                    return -1;

                Task.Delay(1000);
                return GetCount(whereClause, --retriesAllowed);
            }
        }

        /// <summary>
        /// Determines whether the specified where clause has results.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        /// <returns>
        ///   <c>true</c> if the specified where clause has results; otherwise, <c>false</c>.
        /// </returns>
        public bool HasResults(string whereClause)
        {
            var query = $"SELECT TOP 1 c.id FROM c WHERE {whereClause}";
            var uri = UriFactory.CreateDocumentCollectionUri(_client.DatabaseId, _client.CollectionId);
            var feedOptions = _client.EnableCrossPartitionQuery ? new FeedOptions { EnableCrossPartitionQuery = true } : null;

            return _client.Client.CreateDocumentQuery(uri, query, feedOptions).AsEnumerable().Any();
        }

        /// <summary>
        /// Deletes the documents for the specified where clause.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        /// <returns></returns>
        public IEnumerable<string> Delete(string whereClause)
        {
            var query = $"SELECT TOP 500 c.id AS Id, c.{_client.PartitionKey} AS PartitionKey FROM c WHERE {whereClause}";
            var uri = UriFactory.CreateDocumentCollectionUri(_client.DatabaseId, _client.CollectionId);
            var feedOptions = _client.EnableCrossPartitionQuery ? new FeedOptions {EnableCrossPartitionQuery = true} : null;
            
            var retries = 0;
            do
            {
                List<IdPartitionKeyDto> result = null;

                try
                {
                    result = _client.Client.CreateDocumentQuery<IdPartitionKeyDto>(uri, query, feedOptions).AsEnumerable().ToList();
                }
                catch (Exception e)
                {
                    if (++retries >= 10)
                    {
                        Console.WriteLine("ERROR: TOO MANY RETRIES. FETCHING DOCUMENTS NOT POSSIBLE. TERMINATING.");
                        throw;
                    }

                    Console.WriteLine($"EXCEPTION WHILE FETCHING DOCUMENTS - {e.Message}");
                    Task.Delay(1000);
                }

                if (result != null)
                {
                    retries = 0;
                    foreach (var doc in result)
                    {
                        var docId = doc.Id;
                        var noException = true;

                        try
                        {
                            RequestOptions requestOptions = null;

                            if (_client.EnableCrossPartitionQuery)
                                requestOptions = new RequestOptions()
                                {
                                    PartitionKey = new PartitionKey(doc.PartitionKey)
                                };

                            var docUri = UriFactory.CreateDocumentUri(_client.DatabaseId, _client.CollectionId, docId);
                            _client.Client.DeleteDocumentAsync(docUri, requestOptions).Wait();
                        }
                        catch (Exception e)
                        {
                            noException = false;
                            Console.WriteLine($"EXCEPTION WHILE DELETING {docId} - {e.Message}");
                            Task.Delay(500);
                        }

                        if (noException)
                            yield return docId;
                    }
                }

                Task.Delay(1000);
            } while (HasResults(whereClause));
        }
    }
}
