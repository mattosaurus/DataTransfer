using CsvHelper;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataTransfer.SqlToSftpCsv
{
    class Program
    {
        private const string _schema = "dbo";
        private const string _table = "<YOURTABLENAME>";
        private const string _sqlConnectionString = "<YOURSQLCONNECTIONSTRING>";
        private const string _host = "<YOURSFTPHOST>";
        private const string _userName = "<YOURSFTPUSERNAME>";
        private const string _password = "<YOURSFTPPASSWORD>";
        private const string _remoteOutputPath = "<PATHTOYOURSFTPFILE>";

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
            using (SftpClient destinationSftp = new SftpClient(_host, _userName, _password))
            using (var memoryStream = new MemoryStream())
            using (var streamWriter = new StreamWriter(memoryStream))
            using (var csvWriter = new CsvWriter(streamWriter))
            {
                destinationSftp.Connect();

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

                // Reset memory stream to begining
                memoryStream.Position = 0;

                // Upload memory stream
                destinationSftp.UploadFile(memoryStream, _remoteOutputPath);
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
