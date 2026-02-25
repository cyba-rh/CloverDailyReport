-- Create CLOVERDAILY table
USE Landing_Operational;
GO

-- Drop table if it exists
IF OBJECT_ID('dbo.CLOVERDAILY', 'U') IS NOT NULL
    DROP TABLE dbo.CLOVERDAILY;
GO

-- Create the CLOVERDAILY table
CREATE TABLE dbo.CLOVERDAILY (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    day DATE NOT NULL,
    mid VARCHAR(50) NOT NULL,
    merchant_name VARCHAR(255) NOT NULL,
    amount_sum DECIMAL(18,2) NOT NULL DEFAULT 0,
    amount_count INT NOT NULL DEFAULT 0,
    refund_sum DECIMAL(18,2) NOT NULL DEFAULT 0,
    refund_count INT NOT NULL DEFAULT 0,
    total DECIMAL(18,2) NOT NULL DEFAULT 0,
    first_transaction_date DATETIME2 NULL,
    first_business_transaction_date DATETIME2 NULL,
    mtd_total DECIMAL(18,2) NOT NULL DEFAULT 0,
    ytd_total DECIMAL(18,2) NOT NULL DEFAULT 0,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

-- Create indexes for better performance
CREATE INDEX IX_CLOVERDAILY_day_mid ON dbo.CLOVERDAILY (day, mid);
CREATE INDEX IX_CLOVERDAILY_day ON dbo.CLOVERDAILY (day);
CREATE INDEX IX_CLOVERDAILY_mid ON dbo.CLOVERDAILY (mid);
GO

PRINT 'CLOVERDAILY table created successfully';