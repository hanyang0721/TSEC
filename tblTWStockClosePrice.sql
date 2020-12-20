SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblTWStockClosePrice](
	[Sdate] [varchar](12) NOT NULL,
	[StockNo] [varchar](64) NOT NULL,
	[StockName] [nvarchar](512) NULL,
	[TotalShares] [varchar](64) NULL,
	[TotalAmount] [varchar](64) NULL,
	[nOpen] [varchar](32) NULL,
	[nHigh] [varchar](32) NULL,
	[nLow] [varchar](32) NULL,
	[nClose] [varchar](32) NULL,
	[PriceChange] [varchar](16) NULL,
	[LastVolume] [varchar](32) NULL,
	[MarketType] [varchar](6) NULL,
	[EntryDate] [datetime] NULL,
 CONSTRAINT [PK__tblTWSto__E09140DBEE1300AF] PRIMARY KEY CLUSTERED 
(
	[Sdate] ASC,
	[StockNo] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
