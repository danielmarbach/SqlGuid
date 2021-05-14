using System;
using System.Linq;
using Spectre.Console;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlGuid
{
    class Program
    {
        static readonly string connectionString = "Server=127.0.0.1,1433;Database=guiddebate;User Id=SA;Password=yourStrong(!)Password";

        static Random random = new Random();

        static async Task Main(string[] args)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var truncate = connection.CreateCommand();
            truncate.CommandText = @"
TRUNCATE TABLE [dbo].[TestGUID]
TRUNCATE TABLE [dbo].[TestGUIDNew]
TRUNCATE TABLE [dbo].[TestId]
TRUNCATE TABLE [dbo].[TestCombGuid]
            ";
            await truncate.ExecuteNonQueryAsync();

            await WriteToNewGuidTableDestination(286112, 10000);
            await WriteToCombGuidTableDestination(286112, 10000);
            await WriteToGUIDTableDestination(286112, 10000);
            await WriteToTestIdTableDestination(286112, 10000);

            await IndexStats(connection, "(B) Index rebuild");

            await using var indexRebuild = connection.CreateCommand();
            indexRebuild.CommandText = @"
ALTER INDEX [IX_ProviderGuid] ON [dbo].[TestGUID] REBUILD;
ALTER INDEX [IX_ProviderCombGuid] ON [dbo].[TestCombGUID] REBUILD;
ALTER INDEX [IX_ProviderGuidNew] ON [dbo].[TestGUIDNew] REBUILD;
ALTER INDEX [IX_ProviderId] ON [dbo].[TestId] REBUILD;
            ";
            await truncate.ExecuteNonQueryAsync();

            await IndexStats(connection, "(A) Index rebuild");

            for (int i = 0; i < 10; i++)
            {
                await Task.WhenAll(
                WriteToNewGuidTableDestination(100000, 10000),
                WriteToCombGuidTableDestination(100000, 10000),
                WriteToGUIDTableDestination(100000, 10000),
                WriteToTestIdTableDestination(100000, 10000));

                await IndexStats(connection, $"({i}) Additional inserts");
            }
        }

        private static async Task IndexStats(SqlConnection connection, string tableName)
        {
            await using var stats = connection.CreateCommand();
            stats.CommandText = @"
SELECT dbschemas.[name] as 'Schema',
dbtables.[name] as 'Table',
dbindexes.[name] as 'Index',
indexstats.avg_fragmentation_in_percent,
indexstats.page_count
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS indexstats
INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]
INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]
INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
AND indexstats.index_id = dbindexes.index_id
WHERE indexstats.database_id = DB_ID()
ORDER BY indexstats.avg_fragmentation_in_percent desc
            ";
            DataTable statsTable = new DataTable("StatsTable");
            SqlDataAdapter statsTableAdapter = new SqlDataAdapter(stats);
            statsTableAdapter.Fill(statsTable);

            AnsiConsole.Render(new FigletText(tableName).LeftAligned().Color(Color.Green));

            var table = new Table();

            foreach (DataColumn c in statsTable.Columns)
            {
                table.AddColumn(c.ColumnName);
            }


            foreach (DataRow dr in statsTable.Rows)
            {
                table.AddRow(dr.ItemArray.Select(i => i.ToString()).ToArray());
            }
            table.Expand();
            AnsiConsole.Render(table);
        }

        private static async Task WriteToNewGuidTableDestination(int numberOfItems, int batchSize)
        {
            var tbl = new DataTable();
            using (var copy = new SqlBulkCopy(connectionString))
            {
                TestNewGuidTableDestination(tbl, copy);

                for (int i = 0; i < numberOfItems; i++)
                {
                    if (i % batchSize == 0)
                    {
                        await WriteToServer(tbl, copy);
                    }
                    DataRow dr = tbl.NewRow();
                    dr["Id"] = i;
                    dr["ProviderGuid"] = Guid.NewGuid();
                    dr["ProviderKey"] = "ProviderKey";
                    dr["Key"] = "Key";
                    dr["ProcessDate"] = DateTime.Now;
                    tbl.Rows.Add(dr);
                }

                await WriteToServer(tbl, copy);
            }
        }

        private static async Task WriteToCombGuidTableDestination(int numberOfItems, int batchSize)
        {
            var tbl = new DataTable();
            using (var copy = new SqlBulkCopy(connectionString))
            {
                TestCombGuidTableDestination(tbl, copy);

                for (int i = 0; i < numberOfItems; i++)
                {
                    if (i % batchSize == 0)
                    {
                        await WriteToServer(tbl, copy);
                    }
                    DataRow dr = tbl.NewRow();
                    dr["Id"] = i;
                    dr["ProviderGuid"] = CombGuid.Generate();
                    dr["ProviderKey"] = "ProviderKey";
                    dr["Key"] = "Key";
                    dr["ProcessDate"] = DateTime.Now;

                    tbl.Rows.Add(dr);
                }

                await WriteToServer(tbl, copy);
            }
        }

        static async Task WriteToGUIDTableDestination(int numberOfItems, int batchSize)
        {
            var tbl = new DataTable();
            using (var copy = new SqlBulkCopy(connectionString))
            {
                TestGUIDTableDestination(tbl, copy);

                for (int i = 0; i < numberOfItems; i++)
                {
                    if (i % batchSize == 0)
                    {
                        await WriteToServer(tbl, copy);
                    }

                    DataRow dr = tbl.NewRow();
                    dr["Id"] = i;
                    dr["ProviderKey"] = "ProviderKey";
                    dr["Key"] = "Key";
                    dr["ProcessDate"] = DateTime.Now;

                    tbl.Rows.Add(dr);
                }

                await WriteToServer(tbl, copy);
            }
        }

        static async Task WriteToTestIdTableDestination(int numberOfItems, int batchSize)
        {
            var tbl = new DataTable();
            using (var copy = new SqlBulkCopy(connectionString))
            {
                TestIdTableDestination(tbl, copy);

                for (int i = 0; i < numberOfItems; i++)
                {
                    if (i % batchSize == 0)
                    {
                        await WriteToServer(tbl, copy);
                    }

                    DataRow dr = tbl.NewRow();
                    dr["Id"] = i;
                    dr["ProviderId"] = random.Next(1, 111);
                    dr["ProviderKey"] = "ProviderKey";
                    dr["Key"] = "Key";
                    dr["ProcessDate"] = DateTime.Now;

                    tbl.Rows.Add(dr);
                }

                await WriteToServer(tbl, copy);
            }
        }

        private static void TestIdTableDestination(DataTable tbl, SqlBulkCopy copy)
        {
            copy.DestinationTableName = "dbo.TestId";

            tbl.Columns.Clear();
            tbl.Columns.Add(new DataColumn("Id", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("ProviderId", typeof(int)));
            tbl.Columns.Add(new DataColumn("ProviderKey", typeof(string)));
            tbl.Columns.Add(new DataColumn("Key", typeof(string)));
            tbl.Columns.Add(new DataColumn("ProcessDate", typeof(DateTime)));

            MapColums(tbl, copy);
        }

        private static void TestGUIDTableDestination(DataTable tbl, SqlBulkCopy copy)
        {
            copy.DestinationTableName = "dbo.TestGUID";

            tbl.Columns.Clear();
            tbl.Columns.Add(new DataColumn("Id", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("ProviderKey", typeof(string)));
            tbl.Columns.Add(new DataColumn("Key", typeof(string)));
            tbl.Columns.Add(new DataColumn("ProcessDate", typeof(DateTime)));

            MapColums(tbl, copy);
        }

        private static void TestCombGuidTableDestination(DataTable tbl, SqlBulkCopy copy)
        {
            copy.DestinationTableName = "dbo.TestCombGUID";

            tbl.Columns.Clear();
            tbl.Columns.Add(new DataColumn("Id", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("ProviderGuid", typeof(Guid)));
            tbl.Columns.Add(new DataColumn("ProviderKey", typeof(string)));
            tbl.Columns.Add(new DataColumn("Key", typeof(string)));
            tbl.Columns.Add(new DataColumn("ProcessDate", typeof(DateTime)));

            MapColums(tbl, copy);
        }

        private static void TestNewGuidTableDestination(DataTable tbl, SqlBulkCopy copy)
        {
            copy.DestinationTableName = "dbo.TestGUIDNew";

            tbl.Columns.Clear();
            tbl.Columns.Add(new DataColumn("Id", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("ProviderGuid", typeof(Guid)));
            tbl.Columns.Add(new DataColumn("ProviderKey", typeof(string)));
            tbl.Columns.Add(new DataColumn("Key", typeof(string)));
            tbl.Columns.Add(new DataColumn("ProcessDate", typeof(DateTime)));

            MapColums(tbl, copy);
        }

        private static async Task WriteToServer(DataTable tbl, SqlBulkCopy copy)
        {
            await copy.WriteToServerAsync(tbl);
            tbl.Rows.Clear();
        }

        private static void MapColums(DataTable tbl, SqlBulkCopy copy)
        {
            copy.ColumnMappings.Clear();
            foreach (System.Data.DataColumn c in tbl.Columns)
            {
                copy.ColumnMappings.Add(c.ColumnName, c.ColumnName);
            }
        }
    }

    /// <summary>
    /// Generates a Guid using http://www.informit.com/articles/article.asp?p=25862
    /// The Comb algorithm is designed to make the use of <see cref="Guid" />s as Primary Keys, Foreign Keys, and Indexes
    /// nearly as efficient
    /// as <see cref="int" />.
    /// </summary>
    /// <remarks>Source: https://github.com/nhibernate/nhibernate-core/blob/4.0.4.GA/src/NHibernate/Id/GuidCombGenerator.cs</remarks>
    static class CombGuid
    {
        /// <summary>
        /// Generate a new <see cref="Guid" /> using the comb algorithm.
        /// </summary>
        public static Guid Generate()
        {
            var guidArray = Guid.NewGuid().ToByteArray();

            var now = DateTime.UtcNow;

            // Get the days and milliseconds which will be used to build the byte string
            var days = new TimeSpan(now.Ticks - BaseDateTicks);
            var timeOfDay = now.TimeOfDay;

            // Convert to a byte array
            // Note that SQL Server is accurate to 1/300th of a millisecond so we divide by 3.333333
            var daysArray = BitConverter.GetBytes(days.Days);
            var millisecondArray = BitConverter.GetBytes((long)(timeOfDay.TotalMilliseconds / 3.333333));

            // Reverse the bytes to match SQL Servers ordering
            Array.Reverse(daysArray);
            Array.Reverse(millisecondArray);

            // Copy the bytes into the guid
            Array.Copy(daysArray, daysArray.Length - 2, guidArray, guidArray.Length - 6, 2);
            Array.Copy(millisecondArray, millisecondArray.Length - 4, guidArray, guidArray.Length - 4, 4);

            return new Guid(guidArray);
        }

        static readonly long BaseDateTicks = new DateTime(1900, 1, 1).Ticks;
    }
}
