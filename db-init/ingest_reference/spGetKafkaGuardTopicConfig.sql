USE [ingest_reference];
GO

CREATE OR ALTER PROCEDURE [dbo].[spGetKafkaGuardTopicConfig]
    @ConnectorName VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@ConnectorName)), '') IS NULL
    BEGIN
        THROW 50001, 'ConnectorName must not be empty.', 1;
    END;

    SELECT
        topic.[ConnectorName],
        topic.[ListCDCTopic],
        topic.[ConfigID],
        config.[ConfiguredValue] AS [DatabaseCredential]
    FROM [dbo].[KafkaGuardTopic] AS topic
    INNER JOIN [dbo].[ETLConfiguration] AS config
        ON config.[ETLConfigurationID] = topic.[ConfigID]
    WHERE topic.[ConnectorName] = @ConnectorName;
END;
GO
