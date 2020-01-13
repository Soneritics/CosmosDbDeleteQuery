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
    }
}
