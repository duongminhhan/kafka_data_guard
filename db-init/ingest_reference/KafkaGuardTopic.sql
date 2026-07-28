IF DB_ID(N'ingest_reference') IS NULL
BEGIN
    CREATE DATABASE [ingest_reference];
END;
GO

USE [ingest_reference];
GO

-- UAT đã có bảng này. Khối dưới chỉ giúp môi trường POC mới khởi tạo được.
IF OBJECT_ID(N'[dbo].[ETLConfiguration]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[ETLConfiguration]
    (
        [ETLConfigurationID] INT IDENTITY(1,1) NOT NULL,
        [ConfigurationFilter] VARCHAR(255) NOT NULL,
        [QueueName] VARCHAR(50) NULL,
        [ConfiguredDriver] VARCHAR(100) NULL,
        [ConfiguredValue] VARCHAR(750) NULL,
        [Description] NVARCHAR(255) NULL,
        CONSTRAINT [PK_ETLConfiguration]
            PRIMARY KEY ([ETLConfigurationID])
    );
END;
GO

IF OBJECT_ID(N'[dbo].[KafkaGuardTopic]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[KafkaGuardTopic]
    (
        [ID] UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT [DF_KafkaGuardTopic_ID] DEFAULT NEWID(),
        [ConnectorName] VARCHAR(255) NOT NULL,
        [ListCDCTopic] NVARCHAR(MAX) NOT NULL,
        [ConfigID] INT NOT NULL,
        [CreatedAt] DATETIME2(3) NOT NULL
            CONSTRAINT [DF_KafkaGuardTopic_CreatedAt] DEFAULT SYSUTCDATETIME(),
        [UpdatedAt] DATETIME2(3) NOT NULL
            CONSTRAINT [DF_KafkaGuardTopic_UpdatedAt] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_KafkaGuardTopic]
            PRIMARY KEY ([ID]),
        CONSTRAINT [UQ_KafkaGuardTopic_ConnectorName]
            UNIQUE ([ConnectorName]),
        CONSTRAINT [CK_KafkaGuardTopic_ListCDCTopic_NotEmpty]
            CHECK (NULLIF(LTRIM(RTRIM([ListCDCTopic])), '') IS NOT NULL),
        CONSTRAINT [FK_KafkaGuardTopic_ETLConfiguration]
            FOREIGN KEY ([ConfigID])
            REFERENCES [dbo].[ETLConfiguration] ([ETLConfigurationID])
    );
END;
GO

-- Migration từ contract JSON array cũ sang chuỗi CSV.
IF EXISTS
(
    SELECT 1
    FROM sys.check_constraints
    WHERE [name] = N'CK_KafkaGuardTopic_ListCDCTopic_IsJson'
      AND [parent_object_id] = OBJECT_ID(N'[dbo].[KafkaGuardTopic]')
)
BEGIN
    ALTER TABLE [dbo].[KafkaGuardTopic]
        DROP CONSTRAINT [CK_KafkaGuardTopic_ListCDCTopic_IsJson];
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.check_constraints
    WHERE [name] = N'CK_KafkaGuardTopic_ListCDCTopic_NotEmpty'
      AND [parent_object_id] = OBJECT_ID(N'[dbo].[KafkaGuardTopic]')
)
BEGIN
    ALTER TABLE [dbo].[KafkaGuardTopic]
        ADD CONSTRAINT [CK_KafkaGuardTopic_ListCDCTopic_NotEmpty]
        CHECK (NULLIF(LTRIM(RTRIM([ListCDCTopic])), '') IS NOT NULL);
END;
GO
