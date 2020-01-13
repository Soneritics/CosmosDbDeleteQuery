using System;
using System.Text;
using System.Threading.Tasks;
using CosmosDbDeleteQuery.Connection;

namespace CosmosDbDeleteQuery
{
    class Program
    {
        static async Task Main(string[] args)
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

                    Console.WriteLine("All done.");
                }

                proceed = GetBoolVal("Execute another query", false);
            }
        }

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

        private static string GetStringVal(string label, string defaultValue = "")
        {
            Console.Write($"{label}: ({defaultValue}) ");
            var val = Console.ReadLine();

            return string.IsNullOrEmpty(val) ? defaultValue : val;
        }

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
    }
}
