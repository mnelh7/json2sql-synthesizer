using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

if (args.Length == 0 || args[0] is "-h" or "--help")
{
    Console.WriteLine("Usage: ingest <path-to-json> --machine-id <id> [--force]");
    return;
}

string filePath = args[0];
string machineId = "M01";
bool force = args.Any(a => a.Equals("--force", StringComparison.OrdinalIgnoreCase));

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
    // 1) Find existing ingestion by hash (if any)
    int? existingIngestionId = null;
    string? existingStatus = null;

    await using (var findCmd = new SqlCommand(
        "SELECT TOP 1 IngestionId, ImportStatus FROM dbo.IngestionLog WHERE FileHashSha256 = @hash ORDER BY IngestionId DESC",
        conn,
        (SqlTransaction)tx))
    {
        findCmd.Parameters.AddWithValue("@hash", fileHash);
        await using var r = await findCmd.ExecuteReaderAsync();
        if (await r.ReadAsync())
        {
            existingIngestionId = r.GetInt32(0);
            existingStatus = r.GetString(1);
        }
    }

    int ingestionId;

    if (existingIngestionId.HasValue)
    {
        if (!force && string.Equals(existingStatus, "Imported", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("File already imported. Skipping.");
            await tx.RollbackAsync();
            return;
        }

        ingestionId = existingIngestionId.Value;

        await using (var setProcessing = new SqlCommand(
            "UPDATE dbo.IngestionLog SET ImportStatus='Processing', ErrorMessage=NULL WHERE IngestionId=@id",
            conn,
            (SqlTransaction)tx))
        {
            setProcessing.Parameters.AddWithValue("@id", ingestionId);
            await setProcessing.ExecuteNonQueryAsync();
        }

        Console.WriteLine($"Reprocessing existing ingestion: {ingestionId} (force={force})");
    }
    else
    {
        // 2) Insert IngestionLog (Processing)
        await using var insertLogCmd = new SqlCommand(@"
            INSERT INTO dbo.IngestionLog (MachineId, SourceFileName, FileHashSha256, ImportStatus)
            OUTPUT INSERTED.IngestionId
            VALUES (@machineId, @fileName, @hash, 'Processing')",
            conn,
            (SqlTransaction)tx);

        insertLogCmd.Parameters.AddWithValue("@machineId", machineId);
        insertLogCmd.Parameters.AddWithValue("@fileName", Path.GetFileName(filePath));
        insertLogCmd.Parameters.AddWithValue("@hash", fileHash);

        ingestionId = (int)await insertLogCmd.ExecuteScalarAsync();

        // 3) Insert RawPayload
        await using var insertRawCmd = new SqlCommand(@"
            INSERT INTO dbo.RawPayload (IngestionId, RawJson)
            VALUES (@ingestionId, @rawJson)",
            conn,
            (SqlTransaction)tx);

        insertRawCmd.Parameters.AddWithValue("@ingestionId", ingestionId);
        insertRawCmd.Parameters.AddWithValue("@rawJson", rawJson);
        await insertRawCmd.ExecuteNonQueryAsync();
    }

    // 4) Parse JSON root + pkzData
    using var doc = JsonDocument.Parse(rawJson);
    var root = doc.RootElement;

    string timespan = root.TryGetProperty("timespan", out var timespanEl) ? (timespanEl.GetString() ?? "") : "";

    DateTime? payloadStartUtc = null;
    DateTime? payloadEndUtc = null;

    if (root.TryGetProperty("timeStamps", out var timeStampsEl))
    {
        payloadStartUtc = TryParseUtc(timeStampsEl, "start");
        payloadEndUtc = TryParseUtc(timeStampsEl, "end");
    }

    if (!root.TryGetProperty("pkzData", out var pkzDataEl) || pkzDataEl.ValueKind != JsonValueKind.Array)
        throw new InvalidOperationException("Invalid JSON: pkzData missing or not an array.");

    // MERGE SQL
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

    const string mergeStatusIntervalSql = @"
MERGE dbo.StatusInterval AS tgt
USING (
    SELECT @MachineId AS MachineId,
           @HourStartUtc AS HourStartUtc,
           @StatusName AS StatusName,
           @IntervalStartUtc AS IntervalStartUtc,
           @IntervalEndUtc AS IntervalEndUtc
) AS src
ON tgt.MachineId = src.MachineId
AND tgt.HourStartUtc = src.HourStartUtc
AND tgt.StatusName = src.StatusName
AND tgt.IntervalStartUtc = src.IntervalStartUtc
AND tgt.IntervalEndUtc = src.IntervalEndUtc
WHEN MATCHED THEN
    UPDATE SET DurationSec=@DurationSec, IngestionId=@IngestionId
WHEN NOT MATCHED THEN
    INSERT (MachineId, HourStartUtc, StatusName, IntervalStartUtc, IntervalEndUtc, DurationSec, IngestionId)
    VALUES (@MachineId, @HourStartUtc, @StatusName, @IntervalStartUtc, @IntervalEndUtc, @DurationSec, @IngestionId);";

    const string mergeProdSummarySql = @"
MERGE dbo.ProdSummary AS tgt
USING (SELECT @MachineId AS MachineId, @HourStartUtc AS HourStartUtc, @RecipeName AS RecipeName) AS src
ON tgt.MachineId = src.MachineId AND tgt.HourStartUtc = src.HourStartUtc AND tgt.RecipeName = src.RecipeName
WHEN MATCHED THEN
    UPDATE SET IoParts=@IoParts, NioParts=@NioParts, TotalParts=@TotalParts, IngestionId=@IngestionId
WHEN NOT MATCHED THEN
    INSERT (MachineId, HourStartUtc, RecipeName, IoParts, NioParts, TotalParts, IngestionId)
    VALUES (@MachineId, @HourStartUtc, @RecipeName, @IoParts, @NioParts, @TotalParts, @IngestionId);";

    const string mergeProdIntervalSql = @"
MERGE dbo.ProdInterval AS tgt
USING (
    SELECT @MachineId AS MachineId,
           @HourStartUtc AS HourStartUtc,
           @RecipeName AS RecipeName,
           @IntervalStartUtc AS IntervalStartUtc,
           @IntervalEndUtc AS IntervalEndUtc
) AS src
ON tgt.MachineId = src.MachineId
AND tgt.HourStartUtc = src.HourStartUtc
AND tgt.RecipeName = src.RecipeName
AND tgt.IntervalStartUtc = src.IntervalStartUtc
AND tgt.IntervalEndUtc = src.IntervalEndUtc
WHEN MATCHED THEN
    UPDATE SET DurationSec=@DurationSec, IngestionId=@IngestionId
WHEN NOT MATCHED THEN
    INSERT (MachineId, HourStartUtc, RecipeName, IntervalStartUtc, IntervalEndUtc, DurationSec, IngestionId)
    VALUES (@MachineId, @HourStartUtc, @RecipeName, @IntervalStartUtc, @IntervalEndUtc, @DurationSec, @IngestionId);";

    int hourRows = 0;
    int statusSummaryRows = 0;
    int statusIntervalRows = 0;
    int prodSummaryRows = 0;
    int prodIntervalRows = 0;

    foreach (var hourObj in pkzDataEl.EnumerateArray())
    {
        var hourStartUtc = TryParseUtcRequired(hourObj, "timeStamp");

        // Upsert Hour
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

        // StatusSummary + StatusInterval
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

                await using (var cmd = new SqlCommand(mergeStatusSummarySql, conn, (SqlTransaction)tx))
                {
                    cmd.Parameters.AddWithValue("@MachineId", machineId);
                    cmd.Parameters.AddWithValue("@HourStartUtc", hourStartUtc);
                    cmd.Parameters.AddWithValue("@StatusName", statusName);
                    cmd.Parameters.AddWithValue("@Count", count);
                    cmd.Parameters.AddWithValue("@TimeSum", timeSum);
                    cmd.Parameters.AddWithValue("@IngestionId", ingestionId);
                    await cmd.ExecuteNonQueryAsync();
                    statusSummaryRows++;
                }

                if (statusRec.TryGetProperty("timeStamps", out var intervalsEl) && intervalsEl.ValueKind == JsonValueKind.Array)
                {
                    foreach (var intervalObj in intervalsEl.EnumerateArray())
                    {
                        var startUtc = TryParseUtcRequired(intervalObj, "start");
                        var endUtc = TryParseUtcRequired(intervalObj, "end");
                        double durationSec = (endUtc - startUtc).TotalSeconds;

                        await using var cmd = new SqlCommand(mergeStatusIntervalSql, conn, (SqlTransaction)tx);
                        cmd.Parameters.AddWithValue("@MachineId", machineId);
                        cmd.Parameters.AddWithValue("@HourStartUtc", hourStartUtc);
                        cmd.Parameters.AddWithValue("@StatusName", statusName);
                        cmd.Parameters.AddWithValue("@IntervalStartUtc", startUtc);
                        cmd.Parameters.AddWithValue("@IntervalEndUtc", endUtc);
                        cmd.Parameters.AddWithValue("@DurationSec", durationSec);
                        cmd.Parameters.AddWithValue("@IngestionId", ingestionId);
                        await cmd.ExecuteNonQueryAsync();
                        statusIntervalRows++;
                    }
                }
            }
        }

        // Production: ProdSummary + ProdInterval
        if (hourObj.TryGetProperty("prodData", out var prodDataEl) && prodDataEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var prodRec in prodDataEl.EnumerateArray())
            {
                string recipeName = prodRec.TryGetProperty("name", out var nameEl) ? (nameEl.GetString() ?? "") : "";
                if (string.IsNullOrWhiteSpace(recipeName)) continue;

                int io = 0, nio = 0, total = 0;
                if (prodRec.TryGetProperty("partData", out var partEl) && partEl.ValueKind == JsonValueKind.Object)
                {
                    io = partEl.TryGetProperty("amountIoParts", out var ioEl) && ioEl.ValueKind == JsonValueKind.Number ? ioEl.GetInt32() : 0;
                    nio = partEl.TryGetProperty("amountNioParts", out var nioEl) && nioEl.ValueKind == JsonValueKind.Number ? nioEl.GetInt32() : 0;
                    total = partEl.TryGetProperty("amountTotalParts", out var tEl2) && tEl2.ValueKind == JsonValueKind.Number ? tEl2.GetInt32() : 0;
                }

                await using (var cmd = new SqlCommand(mergeProdSummarySql, conn, (SqlTransaction)tx))
                {
                    cmd.Parameters.AddWithValue("@MachineId", machineId);
                    cmd.Parameters.AddWithValue("@HourStartUtc", hourStartUtc);
                    cmd.Parameters.AddWithValue("@RecipeName", recipeName);
                    cmd.Parameters.AddWithValue("@IoParts", io);
                    cmd.Parameters.AddWithValue("@NioParts", nio);
                    cmd.Parameters.AddWithValue("@TotalParts", total);
                    cmd.Parameters.AddWithValue("@IngestionId", ingestionId);
                    await cmd.ExecuteNonQueryAsync();
                    prodSummaryRows++;
                }

                if (prodRec.TryGetProperty("timeStamps", out var prodIntervalsEl) && prodIntervalsEl.ValueKind == JsonValueKind.Array)
                {
                    foreach (var intervalObj in prodIntervalsEl.EnumerateArray())
                    {
                        var startUtc = TryParseUtcRequired(intervalObj, "start");
                        var endUtc = TryParseUtcRequired(intervalObj, "end");
                        double durationSec = (endUtc - startUtc).TotalSeconds;

                        await using var cmd = new SqlCommand(mergeProdIntervalSql, conn, (SqlTransaction)tx);
                        cmd.Parameters.AddWithValue("@MachineId", machineId);
                        cmd.Parameters.AddWithValue("@HourStartUtc", hourStartUtc);
                        cmd.Parameters.AddWithValue("@RecipeName", recipeName);
                        cmd.Parameters.AddWithValue("@IntervalStartUtc", startUtc);
                        cmd.Parameters.AddWithValue("@IntervalEndUtc", endUtc);
                        cmd.Parameters.AddWithValue("@DurationSec", durationSec);
                        cmd.Parameters.AddWithValue("@IngestionId", ingestionId);
                        await cmd.ExecuteNonQueryAsync();
                        prodIntervalRows++;
                    }
                }
            }
        }
    }

    // Update IngestionLog metadata + mark Imported
    await using (var updateLogCmd = new SqlCommand(@"
        UPDATE dbo.IngestionLog
        SET PayloadTimespan = @Timespan,
            PayloadStartUtc = @PayloadStartUtc,
            PayloadEndUtc = @PayloadEndUtc,
            ImportStatus = 'Imported',
            ErrorMessage = NULL
        WHERE IngestionId = @IngestionId;",
        conn,
        (SqlTransaction)tx))
    {
        updateLogCmd.Parameters.AddWithValue("@Timespan", (object)timespan ?? DBNull.Value);
        updateLogCmd.Parameters.AddWithValue("@PayloadStartUtc", (object?)payloadStartUtc ?? DBNull.Value);
        updateLogCmd.Parameters.AddWithValue("@PayloadEndUtc", (object?)payloadEndUtc ?? DBNull.Value);
        updateLogCmd.Parameters.AddWithValue("@IngestionId", ingestionId);
        await updateLogCmd.ExecuteNonQueryAsync();
    }

    await tx.CommitAsync();

    Console.WriteLine($"Import successful. IngestionId = {ingestionId}");
    Console.WriteLine($"Upserted Hour rows: {hourRows}");
    Console.WriteLine($"Upserted StatusSummary rows: {statusSummaryRows}");
    Console.WriteLine($"Upserted StatusInterval rows: {statusIntervalRows}");
    Console.WriteLine($"Upserted ProdSummary rows: {prodSummaryRows}");
    Console.WriteLine($"Upserted ProdInterval rows: {prodIntervalRows}");
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
    return DateTimeOffset.Parse(s).UtcDateTime;
}

static DateTime TryParseUtcRequired(JsonElement obj, string propName)
{
    var dt = TryParseUtc(obj, propName);
    if (dt is null) throw new InvalidOperationException($"Invalid JSON: required datetime '{propName}' missing.");
    return dt.Value;
}