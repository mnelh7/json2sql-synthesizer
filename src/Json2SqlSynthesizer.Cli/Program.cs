using Microsoft.Data.SqlClient;

static string GetConnString()
{
    // Minimal PoC: read from env var first, else fallback to appsettings-ish default.
    // We'll formalize config in Milestone 1.
    var env = Environment.GetEnvironmentVariable("HMI_SQL_CONNECTION");
    if (!string.IsNullOrWhiteSpace(env)) return env;

    // Default: SQLEXPRESS (change if you're using LocalDB)
    return @"Server=.\SQLEXPRESS;Database=HmiAnalytics;Trusted_Connection=True;TrustServerCertificate=True";
}

var cs = GetConnString();

try
{
    await using var conn = new SqlConnection(cs);
    await conn.OpenAsync();

    await using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT DB_NAME()";
    var db = (string?)await cmd.ExecuteScalarAsync();

    Console.WriteLine($"OK: Connected to database '{db}'");
    Environment.ExitCode = 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine("FAILED to connect to SQL.");
    Console.Error.WriteLine(ex.ToString());
    Environment.ExitCode = 1;
}