using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        private class IdDto
        {
            public string id { get; set; }
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
        /// <returns></returns>
        public int GetCount(string whereClause)
        {
            var query = $"SELECT VALUE COUNT(1) FROM c WHERE {whereClause}";
            var uri = UriFactory.CreateDocumentCollectionUri(_client.DatabaseId, _client.CollectionId);
            var feedOptions = _client.EnableCrossPartitionQuery ? new FeedOptions {EnableCrossPartitionQuery = true} : null;

            var result = _client.Client.CreateDocumentQuery(uri, query, feedOptions).AsEnumerable().FirstOrDefault();

            return (int) result;
        }

        /// <summary>
        /// Deletes the documents for the specified where clause.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        /// <returns></returns>
        public IEnumerable<string> Delete(string whereClause)
        {
            var query = $"SELECT c.id FROM c WHERE {whereClause}";
            var uri = UriFactory.CreateDocumentCollectionUri(_client.DatabaseId, _client.CollectionId);
            var feedOptions = _client.EnableCrossPartitionQuery ? new FeedOptions {EnableCrossPartitionQuery = true} : null;

            var result = _client.Client.CreateDocumentQuery(uri, query, feedOptions).AsEnumerable();
            foreach (IdDto doc in result)
            {
                var docId = doc.id;
                var noException = true;

                try
                {
                    
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
    }
}
