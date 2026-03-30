-- SQL Server CREATE TABLE statement generated from MerchantAccounts_20260330_143956.csv
CREATE TABLE [Landing_Operational].[dbo].[CLOVERMERCHANTS] (
    [MerchantGroupID]              BIGINT,
    [MerchantGroupName]            VARCHAR(255),
    [MerchantGroupAddress]         VARCHAR(255),
    [MerchantGroupCity]            VARCHAR(100),
    [MerchantGroupState]           VARCHAR(50),
    [MerchantGroupZip]             VARCHAR(20),
    [MerchantAccountDatabaseID]    INT PRIMARY KEY,
    [MerchantAccountName]          VARCHAR(255),
    [MerchantAccountGatewayID]     VARCHAR(50),
    [MerchantAccountUUID]          VARCHAR(50),
    [MerchantAccountMID]           BIGINT,
    [MerchantAccountGatewayMerchantID] VARCHAR(50),
    [MerchantAccountFiservSalesID] BIGINT,
    [MerchantAccountAddress]       VARCHAR(255),
    [MerchantAccountCity]          VARCHAR(100),
    [MerchantAccountState]         VARCHAR(50),
    [MerchantAccountZip]           VARCHAR(20),
    [MerchantAccountCreated]       DATETIME,
    [ContactEmail]                 VARCHAR(255),
    [ContactFirstName]             VARCHAR(100),
    [ContactLastName]              VARCHAR(100)
);
