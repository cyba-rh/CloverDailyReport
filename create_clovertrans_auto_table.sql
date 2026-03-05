-- SQL Server CREATE TABLE statement generated from CloverDaily_20260225_145155.csv
CREATE TABLE [Landing_Operational].[dbo].[CLOVERTRANS_AUTO] (
    [transaction_uuid] VARCHAR(50) PRIMARY KEY,
    [merchant_account_id] INT,
    [merchant_name] VARCHAR(255),
    [mid] BIGINT,
    [gateway_merchant_id] VARCHAR(50),
    [amount] DECIMAL(18,2),
    [currency] VARCHAR(10),
    [refunded_amount] DECIMAL(18,2),
    [status] VARCHAR(50),
    [source_brand] VARCHAR(50),
    [source_first6] VARCHAR(10),
    [payment_method_details] VARCHAR(255),
    [transaction_date] DATETIME,
    [transaction_type] VARCHAR(50),
    [user_source] VARCHAR(50),
    [SourceAccountCreated] DATETIME,
    [created_at] DATETIME,
    [updated_at] DATETIME
);
