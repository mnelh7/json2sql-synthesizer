using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

if (args.Length == 0)
{
    Console.WriteLine("Usage: ingest <path-to-json> --machine-id <id>");
    return;
}

string filePath = args[0];
string machineId = "M01";

for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--machine-id" && i + 1 < args.Length)
        machineId = args[i + 1];
}

if (!File.Exists(filePath))
{
    Console.WriteLine("File not found.");
    return;
}

string rawJson = await File.ReadAllTextAsync(filePath);
string fileHash = ComputeSha256(rawJson);

string connectionString =
    @"Server=.\SQLEXPRESS;Database=HmiAnalytics;Trusted_Connection=True;TrustServerCertificate=True";

await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

await using var tx = await conn.BeginTransactionAsync();

try
{
    // Check if hash exists
    var checkCmd = new SqlCommand(
        "SELECT COUNT(*) FROM dbo.IngestionLog WHERE FileHashSha256 = @hash",
        conn,
        (SqlTransaction)tx);

    checkCmd.Parameters.AddWithValue("@hash", fileHash);

    int existing = (int)await checkCmd.ExecuteScalarAsync();

    if (existing > 0)
    {
        Console.WriteLine("File already imported. Skipping.");
        await tx.RollbackAsync();
        return;
    }

    // Insert into IngestionLog
    var insertLogCmd = new SqlCommand(@"
        INSERT INTO dbo.IngestionLog
        (MachineId, SourceFileName, FileHashSha256, ImportStatus)
        OUTPUT INSERTED.IngestionId
        VALUES (@machineId, @fileName, @hash, 'Imported')",
        conn,
        (SqlTransaction)tx);

    insertLogCmd.Parameters.AddWithValue("@machineId", machineId);
    insertLogCmd.Parameters.AddWithValue("@fileName", Path.GetFileName(filePath));
    insertLogCmd.Parameters.AddWithValue("@hash", fileHash);

    int ingestionId = (int)await insertLogCmd.ExecuteScalarAsync();

    // Insert raw JSON
    var insertRawCmd = new SqlCommand(@"
        INSERT INTO dbo.RawPayload (IngestionId, RawJson)
        VALUES (@ingestionId, @rawJson)",
        conn,
        (SqlTransaction)tx);

    insertRawCmd.Parameters.AddWithValue("@ingestionId", ingestionId);
    insertRawCmd.Parameters.AddWithValue("@rawJson", rawJson);

    await insertRawCmd.ExecuteNonQueryAsync();

    await tx.CommitAsync();

    Console.WriteLine($"Import successful. IngestionId = {ingestionId}");
}
catch (Exception ex)
{
    await tx.RollbackAsync();
    Console.WriteLine("Import failed:");
    Console.WriteLine(ex.Message);
}

static string ComputeSha256(string raw)
{
    using var sha = SHA256.Create();
    var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(raw));
    return Convert.ToHexString(bytes);
}