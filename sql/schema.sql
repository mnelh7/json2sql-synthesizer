-- HmiAnalytics schema (PoC)

IF DB_ID('HmiAnalytics') IS NULL
BEGIN
    CREATE DATABASE HmiAnalytics;
END
GO

USE HmiAnalytics;
GO

-- 1) IngestionLog
IF OBJECT_ID('dbo.IngestionLog', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.IngestionLog (
        IngestionId INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        MachineId NVARCHAR(50) NULL,
        SourceFileName NVARCHAR(260) NOT NULL,
        FileHashSha256 CHAR(64) NOT NULL,
        PayloadTimespan NVARCHAR(20) NULL,
        PayloadStartUtc DATETIME2 NULL,
        PayloadEndUtc DATETIME2 NULL,
        ImportedAtUtc DATETIME2 NOT NULL CONSTRAINT DF_IngestionLog_ImportedAtUtc DEFAULT SYSUTCDATETIME(),
        ImportStatus NVARCHAR(20) NOT NULL,
        ErrorMessage NVARCHAR(MAX) NULL
    );

    CREATE UNIQUE INDEX UX_IngestionLog_FileHash ON dbo.IngestionLog(FileHashSha256);
END
GO

-- 2) RawPayload (bronze)
IF OBJECT_ID('dbo.RawPayload', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.RawPayload (
        RawPayloadId INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        IngestionId INT NOT NULL,
        RawJson NVARCHAR(MAX) NOT NULL,
        InsertedAtUtc DATETIME2 NOT NULL CONSTRAINT DF_RawPayload_InsertedAtUtc DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_RawPayload_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );
END
GO

-- 3) Hour
IF OBJECT_ID('dbo.[Hour]', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.[Hour] (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        Timespan NVARCHAR(20) NULL,
        PayloadStartUtc DATETIME2 NULL,
        PayloadEndUtc DATETIME2 NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_Hour PRIMARY KEY (MachineId, HourStartUtc),
        CONSTRAINT FK_Hour_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );

    CREATE INDEX IX_Hour_HourStartUtc ON dbo.[Hour](HourStartUtc);
END
GO

-- 4) UserShare
IF OBJECT_ID('dbo.UserShare', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.UserShare (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        UserName NVARCHAR(200) NOT NULL,
        Percentage FLOAT NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_UserShare PRIMARY KEY (MachineId, HourStartUtc, UserName),
        CONSTRAINT FK_UserShare_Hour FOREIGN KEY (MachineId, HourStartUtc) REFERENCES dbo.[Hour](MachineId, HourStartUtc),
        CONSTRAINT FK_UserShare_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );
END
GO

-- 5) UserInterval
IF OBJECT_ID('dbo.UserInterval', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.UserInterval (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        UserName NVARCHAR(200) NOT NULL,
        IntervalStartUtc DATETIME2 NOT NULL,
        IntervalEndUtc DATETIME2 NOT NULL,
        DurationSec FLOAT NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_UserInterval PRIMARY KEY (MachineId, HourStartUtc, UserName, IntervalStartUtc, IntervalEndUtc),
        CONSTRAINT FK_UserInterval_Hour FOREIGN KEY (MachineId, HourStartUtc) REFERENCES dbo.[Hour](MachineId, HourStartUtc),
        CONSTRAINT FK_UserInterval_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );

    CREATE INDEX IX_UserInterval_StartUtc ON dbo.UserInterval(IntervalStartUtc);
END
GO

-- 6) ProdSummary
IF OBJECT_ID('dbo.ProdSummary', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ProdSummary (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        RecipeName NVARCHAR(200) NOT NULL,
        IoParts INT NULL,
        NioParts INT NULL,
        TotalParts INT NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_ProdSummary PRIMARY KEY (MachineId, HourStartUtc, RecipeName),
        CONSTRAINT FK_ProdSummary_Hour FOREIGN KEY (MachineId, HourStartUtc) REFERENCES dbo.[Hour](MachineId, HourStartUtc),
        CONSTRAINT FK_ProdSummary_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );
END
GO

-- 7) ProdInterval
IF OBJECT_ID('dbo.ProdInterval', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ProdInterval (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        RecipeName NVARCHAR(200) NOT NULL,
        IntervalStartUtc DATETIME2 NOT NULL,
        IntervalEndUtc DATETIME2 NOT NULL,
        DurationSec FLOAT NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_ProdInterval PRIMARY KEY (MachineId, HourStartUtc, RecipeName, IntervalStartUtc, IntervalEndUtc),
        CONSTRAINT FK_ProdInterval_Hour FOREIGN KEY (MachineId, HourStartUtc) REFERENCES dbo.[Hour](MachineId, HourStartUtc),
        CONSTRAINT FK_ProdInterval_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );

    CREATE INDEX IX_ProdInterval_StartUtc ON dbo.ProdInterval(IntervalStartUtc);
END
GO

-- 8) StatusSummary
IF OBJECT_ID('dbo.StatusSummary', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.StatusSummary (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        StatusName NVARCHAR(50) NOT NULL,
        [Count] INT NULL,
        TimeSum BIGINT NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_StatusSummary PRIMARY KEY (MachineId, HourStartUtc, StatusName),
        CONSTRAINT FK_StatusSummary_Hour FOREIGN KEY (MachineId, HourStartUtc) REFERENCES dbo.[Hour](MachineId, HourStartUtc),
        CONSTRAINT FK_StatusSummary_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );
END
GO

-- 9) StatusInterval
IF OBJECT_ID('dbo.StatusInterval', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.StatusInterval (
        MachineId NVARCHAR(50) NOT NULL,
        HourStartUtc DATETIME2 NOT NULL,
        StatusName NVARCHAR(50) NOT NULL,
        IntervalStartUtc DATETIME2 NOT NULL,
        IntervalEndUtc DATETIME2 NOT NULL,
        DurationSec FLOAT NULL,
        IngestionId INT NOT NULL,
        CONSTRAINT PK_StatusInterval PRIMARY KEY (MachineId, HourStartUtc, StatusName, IntervalStartUtc, IntervalEndUtc),
        CONSTRAINT FK_StatusInterval_Hour FOREIGN KEY (MachineId, HourStartUtc) REFERENCES dbo.[Hour](MachineId, HourStartUtc),
        CONSTRAINT FK_StatusInterval_IngestionLog FOREIGN KEY (IngestionId) REFERENCES dbo.IngestionLog(IngestionId)
    );

    CREATE INDEX IX_StatusInterval_StartUtc ON dbo.StatusInterval(IntervalStartUtc);
END
GO