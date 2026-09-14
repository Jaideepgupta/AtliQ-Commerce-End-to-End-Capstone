-- ============================================================================
-- AtliQ Commerce | Pipeline Audit Logging
-- Records pipeline execution status, timestamps and row counts.
-- ============================================================================

IF OBJECT_ID('etl.pipeline_audit', 'U') IS NOT NULL
    DROP TABLE etl.pipeline_audit;
GO

CREATE TABLE etl.pipeline_audit
(
    audit_id        BIGINT IDENTITY(1,1) NOT NULL,
    run_id          UNIQUEIDENTIFIER NOT NULL,
    pipeline_name   NVARCHAR(200) NOT NULL,
    table_name      NVARCHAR(128) NULL,
    run_start_at    DATETIME2(0) NOT NULL,
    run_end_at      DATETIME2(0) NULL,
    row_count       BIGINT NULL,
    status          NVARCHAR(20) NOT NULL,
    error_message   NVARCHAR(4000) NULL,

    CONSTRAINT PK_pipeline_audit
        PRIMARY KEY (audit_id),

    CONSTRAINT CK_pipeline_audit_status
        CHECK (status IN ('Started', 'Succeeded', 'Failed'))
);
GO

CREATE OR ALTER PROCEDURE etl.usp_log_pipeline_audit
    @run_id          UNIQUEIDENTIFIER,
    @pipeline_name   NVARCHAR(200),
    @table_name      NVARCHAR(128) = NULL,
    @run_start_at    DATETIME2(0),
    @run_end_at      DATETIME2(0) = NULL,
    @row_count       BIGINT = NULL,
    @status          NVARCHAR(20),
    @error_message   NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.pipeline_audit
    (
        run_id,
        pipeline_name,
        table_name,
        run_start_at,
        run_end_at,
        row_count,
        status,
        error_message
    )
    VALUES
    (
        @run_id,
        @pipeline_name,
        @table_name,
        @run_start_at,
        @run_end_at,
        @row_count,
        @status,
        @error_message
    );
END;
GO