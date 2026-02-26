using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

if (args.Length == 0 || args[0] is "-h" or "--help")
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
    Console.WriteLine($"File not found: {filePath}");
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
    // 1) Dedup check
    var checkCmd = new SqlCommand(
        "SELECT COUNT(*) FROM dbo.IngestionLog WHERE FileHashSha256 = @hash AND ImportStatus = 'Imported'",
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

    // 2) Insert IngestionLog (Processing)
    var insertLogCmd = new SqlCommand(@"
        INSERT INTO dbo.IngestionLog (MachineId, SourceFileName, FileHashSha256, ImportStatus)
        OUTPUT INSERTED.IngestionId
        VALUES (@machineId, @fileName, @hash, 'Processing')",
        conn,
        (SqlTransaction)tx);

    insertLogCmd.Parameters.AddWithValue("@machineId", machineId);
    insertLogCmd.Parameters.AddWithValue("@fileName", Path.GetFileName(filePath));
    insertLogCmd.Parameters.AddWithValue("@hash", fileHash);

    int ingestionId = (int)await insertLogCmd.ExecuteScalarAsync();

    // 3) Insert RawPayload
    var insertRawCmd = new SqlCommand(@"
        INSERT INTO dbo.RawPayload (IngestionId, RawJson)
        VALUES (@ingestionId, @rawJson)",
        conn,
        (SqlTransaction)tx);

    insertRawCmd.Parameters.AddWithValue("@ingestionId", ingestionId);
    insertRawCmd.Parameters.AddWithValue("@rawJson", rawJson);
    await insertRawCmd.ExecuteNonQueryAsync();

    // 4) Parse JSON root + pkzData
    using var doc = JsonDocument.Parse(rawJson);
    var root = doc.RootElement;

    string timespan = root.TryGetProperty("timespan", out var timespanEl) ? timespanEl.GetString() ?? "" : "";

    DateTime? payloadStartUtc = null;
    DateTime? payloadEndUtc = null;

    if (root.TryGetProperty("timeStamps", out var timeStampsEl))
    {
        payloadStartUtc = TryParseUtc(timeStampsEl, "start");
        payloadEndUtc = TryParseUtc(timeStampsEl, "end");
    }

    if (!root.TryGetProperty("pkzData", out var pkzDataEl) || pkzDataEl.ValueKind != JsonValueKind.Array)
        throw new InvalidOperationException("Invalid JSON: pkzData missing or not an array.");

    // Prepare MERGE commands
    const string mergeHourSql = @"
MERGE dbo.[Hour] AS tgt
USING (SELECT @MachineId AS MachineId, @HourStartUtc AS HourStartUtc) AS src
ON tgt.MachineId = src.MachineId AND tgt.HourStartUtc = src.HourStartUtc
WHEN MATCHED THEN
    UPDATE SET Timespan=@Timespan, PayloadStartUtc=@PayloadStartUtc, PayloadEndUtc=@PayloadEndUtc, IngestionId=@IngestionId
WHEN NOT MATCHED THEN
    INSERT (MachineId, HourStartUtc, Timespan, PayloadStartUtc, PayloadEndUtc, IngestionId)
    VALUES (@MachineId, @HourStartUtc, @Timespan, @PayloadStartUtc, @PayloadEndUtc, @IngestionId);";

    const string mergeStatusSummarySql = @"
MERGE dbo.StatusSummary AS tgt
USING (SELECT @MachineId AS MachineId, @HourStartUtc AS HourStartUtc, @StatusName AS StatusName) AS src
ON tgt.MachineId = src.MachineId AND tgt.HourStartUtc = src.HourStartUtc AND tgt.StatusName = src.StatusName
WHEN MATCHED THEN
    UPDATE SET [Count]=@Count, TimeSum=@TimeSum, IngestionId=@IngestionId
WHEN NOT MATCHED THEN
    INSERT (MachineId, HourStartUtc, StatusName, [Count], TimeSum, IngestionId)
    VALUES (@MachineId, @HourStartUtc, @StatusName, @Count, @TimeSum, @IngestionId);";

    int hourRows = 0;
    int statusRows = 0;

    foreach (var hourObj in pkzDataEl.EnumerateArray())
    {
        var hourStartUtc = TryParseUtcRequired(hourObj, "timeStamp");

        // 4a) Upsert Hour row
        await using (var cmd = new SqlCommand(mergeHourSql, conn, (SqlTransaction)tx))
        {
            cmd.Parameters.AddWithValue("@MachineId", machineId);
            cmd.Parameters.AddWithValue("@HourStartUtc", hourStartUtc);
            cmd.Parameters.AddWithValue("@Timespan", (object)timespan ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PayloadStartUtc", (object?)payloadStartUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PayloadEndUtc", (object?)payloadEndUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@IngestionId", ingestionId);

            await cmd.ExecuteNonQueryAsync();
            hourRows++;
        }

        // 4b) StatusSummary from statusData (dynamic keys)
        if (hourObj.TryGetProperty("statusData", out var statusDataEl) && statusDataEl.ValueKind == JsonValueKind.Object)
        {
            foreach (var statusProp in statusDataEl.EnumerateObject())
            {
                string statusName = statusProp.Name;
                var statusRec = statusProp.Value;

                int count = statusRec.TryGetProperty("count", out var cEl) && cEl.ValueKind == JsonValueKind.Number
                    ? cEl.GetInt32()
                    : 0;

                long timeSum = statusRec.TryGetProperty("timeSum", out var tEl) && tEl.ValueKind == JsonValueKind.Number
                    ? tEl.GetInt64()
                    : 0L;

                await using var cmd = new SqlCommand(mergeStatusSummarySql, conn, (SqlTransaction)tx);
                cmd.Parameters.AddWithValue("@MachineId", machineId);
                cmd.Parameters.AddWithValue("@HourStartUtc", hourStartUtc);
                cmd.Parameters.AddWithValue("@StatusName", statusName);
                cmd.Parameters.AddWithValue("@Count", count);
                cmd.Parameters.AddWithValue("@TimeSum", timeSum);
                cmd.Parameters.AddWithValue("@IngestionId", ingestionId);

                await cmd.ExecuteNonQueryAsync();
                statusRows++;
            }
        }
    }

    // 5) Update IngestionLog with payload metadata + Imported status
    var updateLogCmd = new SqlCommand(@"
        UPDATE dbo.IngestionLog
        SET PayloadTimespan = @Timespan,
            PayloadStartUtc = @PayloadStartUtc,
            PayloadEndUtc = @PayloadEndUtc,
            ImportStatus = 'Imported',
            ErrorMessage = NULL
        WHERE IngestionId = @IngestionId;",
        conn,
        (SqlTransaction)tx);

    updateLogCmd.Parameters.AddWithValue("@Timespan", (object)timespan ?? DBNull.Value);
    updateLogCmd.Parameters.AddWithValue("@PayloadStartUtc", (object?)payloadStartUtc ?? DBNull.Value);
    updateLogCmd.Parameters.AddWithValue("@PayloadEndUtc", (object?)payloadEndUtc ?? DBNull.Value);
    updateLogCmd.Parameters.AddWithValue("@IngestionId", ingestionId);

    await updateLogCmd.ExecuteNonQueryAsync();

    await tx.CommitAsync();

    Console.WriteLine($"Import successful. IngestionId = {ingestionId}");
    Console.WriteLine($"Upserted Hour rows: {hourRows}");
    Console.WriteLine($"Upserted StatusSummary rows: {statusRows}");
}
catch (Exception ex)
{
    await tx.RollbackAsync();
    Console.WriteLine("Import failed:");
    Console.WriteLine(ex);
}

static string ComputeSha256(string raw)
{
    using var sha = SHA256.Create();
    var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(raw));
    return Convert.ToHexString(bytes);
}

static DateTime? TryParseUtc(JsonElement obj, string propName)
{
    if (!obj.TryGetProperty(propName, out var el)) return null;
    if (el.ValueKind != JsonValueKind.String) return null;

    var s = el.GetString();
    if (string.IsNullOrWhiteSpace(s)) return null;

    // Parse ISO8601 with Z as UTC
    var dto = DateTimeOffset.Parse(s);
    return dto.UtcDateTime;
}

static DateTime TryParseUtcRequired(JsonElement obj, string propName)
{
    var dt = TryParseUtc(obj, propName);
    if (dt is null) throw new InvalidOperationException($"Invalid JSON: required datetime '{propName}' missing.");
    return dt.Value;
}