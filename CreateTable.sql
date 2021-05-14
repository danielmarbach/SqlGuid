USE [guiddebate]
GO

CREATE TABLE [dbo].[TestGUID] (
    [Id] [int] NOT NULL IDENTITY,
    [ProviderGuid] [uniqueidentifier] NOT NULL default newid(),
    [ProviderKey] [varchar](100) NOT NULL,
    [Key] [nvarchar](200) NOT NULL,
    [ProcessDate] [datetime2](4) NULL,
    CONSTRAINT [PK_TestGUID] PRIMARY KEY CLUSTERED ([Id] ASC)
)
 
CREATE TABLE [dbo].[TestGUIDNew] (
    [Id] [int] NOT NULL IDENTITY,
    [ProviderGuid] [uniqueidentifier] NOT NULL,
    [ProviderKey] [varchar](100) NOT NULL,
    [Key] [nvarchar](200) NOT NULL,
    [ProcessDate] [datetime2](4) NULL,
    CONSTRAINT [PK_TestGUIDNew] PRIMARY KEY CLUSTERED ([Id] ASC)
)

CREATE TABLE [dbo].[TestCombGuid] (
    [Id] [int] NOT NULL IDENTITY,
    [ProviderGuid] [uniqueidentifier] NOT NULL,
    [ProviderKey] [varchar](100) NOT NULL,
    [Key] [nvarchar](200) NOT NULL,
    [ProcessDate] [datetime2](4) NULL,
    CONSTRAINT [PK_TestCombGuid] PRIMARY KEY CLUSTERED ([Id] ASC)
)
 
CREATE TABLE [dbo].[TestId] (
    [Id] [int] NOT NULL IDENTITY,
    [ProviderId] [int] NOT NULL,
    [ProviderKey] [varchar](100) NOT NULL,
    [Key] [nvarchar](200) NOT NULL,
    [ProcessDate] [datetime2](4) NULL,
    CONSTRAINT [PK_TestId] PRIMARY KEY CLUSTERED ([Id] ASC)
)
 
CREATE INDEX [IX_ProviderGuid] ON [TestGUID] ([ProviderGuid],[ProviderKey]);
CREATE INDEX [IX_ProviderGuidNew] ON [TestGUIDNew] ([ProviderGuid],[ProviderKey]);
CREATE INDEX [IX_ProviderCombGuid] ON [TestCombGUID] ([ProviderGuid],[ProviderKey]);
CREATE INDEX [IX_ProviderId] ON [TestId] ([ProviderId],[ProviderKey]);