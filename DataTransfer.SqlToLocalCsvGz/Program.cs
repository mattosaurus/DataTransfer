using CsvHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace DataTransfer.SqlToLocalCsvGz
{
    class Program
    {
        private const string _schema = "dbo";
        private const string _table = "<YOURTABLENAME>";
        private const string _sqlConnectionString = "<YOURSQLCONNECTIONSTRING>";
        private const string _outputPath = "<PATHTOYOURLOCALFILE>";

        static int Main(string[] args)
        {
            try
            {
                // Start!
                MainAsync(args).Wait();
                return 0;
            }
            catch
            {
                return 1;
            }
        }

        static async Task MainAsync(string[] args)
        {
            using (var outputFile = File.Create(_outputPath))
            using (var gZipStream = new GZipStream(outputFile, CompressionMode.Compress))
            using (var streamWriter = new StreamWriter(gZipStream))
            using (var csvWriter = new CsvWriter(streamWriter))
            {
                // Quote all fields
                csvWriter.Configuration.ShouldQuote = (field, context) => true;

                bool writeHeaders = true;

                await foreach (DataRow row in GetSqlRowsAsync(_schema, _table))
                {
                    // Write header row
                    if (writeHeaders)
                    {
                        foreach (DataColumn column in row.Table.Columns)
                        {
                            csvWriter.WriteField(column.ColumnName);
                        }

                        await csvWriter.NextRecordAsync();
                        writeHeaders = false;
                    }

                    // Write data rows
                    for (var i = 0; i < row.ItemArray.Length; i++)
                    {
                        csvWriter.WriteField(row[i]);
                    }

                    await csvWriter.NextRecordAsync();
                }
            }
        }

        public static async IAsyncEnumerable<DataRow> GetSqlRowsAsync(string schema, string tableName)
        {
            SqlConnection _sqlConnection = new SqlConnection(_sqlConnectionString);

            using (SqlCommand command = new SqlCommand("select * from " + schema + "." + tableName))
            {
                command.Connection = _sqlConnection;
                await _sqlConnection.OpenAsync();

                DataTable table = new DataTable();

                // Get schema of SQL table
                using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                {
                    table.TableName = tableName;
                    adapter.FillSchema(table, SchemaType.Source);
                }

                // Return rows as they're read
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        DataRow row = table.NewRow();

                        row.Table.Columns
                            .Cast<DataColumn>()
                            .ToList()
                            .ForEach(x => row[x] = reader.GetValue(x.Ordinal));

                        yield return row;
                    }
                }

                _sqlConnection.Close();
            }
        }
    }
}
