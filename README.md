# DataTransfer

This is a solution to demonstrate how to transfer data between various sources.

# Dependencies

The following packages are used within this solution though not all are required for each project.

* [CsvHelper](https://joshclose.github.io/CsvHelper/)
* [SSH.NET](https://github.com/sshnet/SSH.NET)

C# 8.0 is also required to allow usage of IAsyncEnumerable, this may need to be specified manually in your cspoj file by adding the `LangVersion` property if your project defaults to using an older version.

```
<PropertyGroup>
	<OutputType>Exe</OutputType>
	<TargetFramework>netcoreapp3.1</TargetFramework>
	<LangVersion>latest</LangVersion>
</PropertyGroup>
```

# Projects

* [SqlToLocalCsv](https://github.com/mattosaurus/DataTransfer/tree/master/DataTransfer.SqlToLocalCsv): Stream a SQL table to a local CSV file.
* [SqlToSftpCsv](https://github.com/mattosaurus/DataTransfer/tree/master/DataTransfer.SqlToSftpCsv): Stream a SQL table to a CSV file on a remote SFTP server.
* [GoogleBigQueryToGoogleCloudStorage](https://github.com/mattosaurus/DataTransfer/tree/master/DataTransfer.GoogleBigQueryToGoogleCloudStorage): Export a BigQuery table to a CSV file in Google Cloud Storage.
