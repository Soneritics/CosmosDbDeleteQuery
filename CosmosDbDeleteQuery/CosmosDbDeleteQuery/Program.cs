using System;
using System.Text;
using CosmosDbDeleteQuery.Connection;

namespace CosmosDbDeleteQuery
{
    /// <summary>
    /// Dev tool to delete documents from Cosmos DB with an SQL query, similar to a 'DELETE FROM ..' SQL-query.
    /// Use the Microsoft Azure Storage Explorer to write a query, then paste the WHERE-clause in this app.
    /// You are specifically asked for the credentials and endpoint, so making a mistake is a little harder :-)
    ///
    /// The tool has little exception handling. Only where necessary. When too many documents are deleted at once, you might
    /// hit the throttling limits of Cosmos. In that case, just run the same query twice ;-)
    /// </summary>
    class Program
    {
        /// <summary>
        /// Where the magic happe.. err.. begins.
        /// </summary>
        /// <param name="args">The arguments.</param>
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to this Cosmos Db tool, that helps you execute delete queries.");
            Console.WriteLine("WARNING: This tool will actually delete, not soft-delete.");
            Console.WriteLine("Use the Microsoft Azure Storage Explorer to write your query, then use this tool to delete multiple documents.\r\n");
            if (!GetBoolVal("Are you sure you want to proceed?", true))
                return;

            var client = new DataClient(GetDataConnectionClient());
            var proceed = true;
            while (proceed)
            {
                var query = GetWhereClause();

                var count = client.GetCount(query);
                Console.WriteLine($"Your query has {count} result(s).");

                if (count != 0 && GetBoolVal("Are you completely sure you want to delete these documents", false))
                {
                    foreach (var documentId in client.Delete(query))
                        Console.WriteLine($"Deleted: {documentId}");

                    Console.WriteLine("All done.\r\n");
                }

                proceed = GetBoolVal("Execute another query", false);
            }
        }

        #region User Input
        /// <summary>
        /// Gets the data connection client, used to query Cosmos.
        /// </summary>
        /// <returns></returns>
        private static DataConnectionClient GetDataConnectionClient()
        {
            Console.Clear();
            Console.WriteLine("Please first provide us with the connection details.");
            var endpoint = GetStringVal("Endpoint", "https://localhost:8081");
            var key = GetStringVal("Account key", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==");
            var databaseId = GetStringVal("Database", string.Empty);
            var collectionId = GetStringVal("Collection", string.Empty);
            var enableCrossPartitionQuery = GetBoolVal("Enable cross partition querying", false);
            var partitionKey = enableCrossPartitionQuery ? GetStringVal("Cross partition query partition key", "id") : string.Empty;

            return new DataConnectionClient(
                endpoint,
                key,
                databaseId,
                collectionId,
                enableCrossPartitionQuery,
                partitionKey
            );
        }

        /// <summary>
        /// Gets the where clause.
        /// </summary>
        /// <returns></returns>
        private static string GetWhereClause()
        {
            Console.Clear();
            Console.WriteLine("Please write your WHERE clause below. When you enter an empty line, the query will be executed.\r\n\r\n");
            Console.WriteLine("DELETE FROM c WHERE");

            var query = new StringBuilder();
            string input;
            do
            {
                query.Append((input = Console.ReadLine()) + " ");
            } while (!string.IsNullOrEmpty(input));

            var result = query.ToString();
            Console.Clear();
            Console.WriteLine("You entered the following query:\r\n\r\n");
            Console.WriteLine($"DELETE FROM c WHERE {result}");

            if (!GetBoolVal("\r\n\r\nAre you sure you want to execute this query?", true))
                return GetWhereClause();

            return result;
        }
        #endregion

        #region User input helpers
        /// <summary>
        /// Gets a string value from user input.
        /// </summary>
        /// <param name="label">The label.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns></returns>
        private static string GetStringVal(string label, string defaultValue = "")
        {
            Console.Write($"{label}: ({defaultValue}) ");
            var val = Console.ReadLine();

            return string.IsNullOrEmpty(val) ? defaultValue : val;
        }

        /// <summary>
        /// Gets a bool value from user input.
        /// </summary>
        /// <param name="label">The label.</param>
        /// <param name="defaultValue">if set to <c>true</c> [default value].</param>
        /// <returns></returns>
        private static bool GetBoolVal(string label, bool defaultValue)
        {
            Console.Write($"{label}: y/n (" + (defaultValue ? "y" : "n") + ") ");
            var val = Console.ReadLine();

            if (string.IsNullOrEmpty(val))
                return defaultValue;

            var v = val.ToLower();

            if (v == "y")
                return true;
            else if (v == "n")
                return false;

            Console.WriteLine("Invalid value. Typ y or n. Try again.");
            return GetBoolVal(label, defaultValue);
        }
        #endregion
    }
}
