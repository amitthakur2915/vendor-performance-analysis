import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True   
)

def create_vendor_summary(conn):
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
     SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price As ActualPrice,
        pp.Volume,
        Sum(p.Quantity) As TotalPurchaseQuantity,
        Sum(p.Dollars) As TotalPurchaseDollars
    From purchases p
    Join purchase_prices pp
        On p.Brand = pp.Brand
    Where p.PurchasePrice > 0
    Group By p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
 ),
 SalesSummary As (
     Select
         VendorNo,
         Brand,
         Sum(SalesQuantity) As TotalSalesQuantity,
         Sum(SalesDollars) As TotalSalesDollars,
         Sum(SalesPrice) As TotalSalesPrice,
         Sum(ExciseTax) As TotalExciseTax
     From sales
     Group By VendorNo, Brand
)

Select
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
 From PurchaseSummary ps
 Left Join SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    And ps.Brand = ss.Brand
 Left Join FreightSummary fs
    On ps.VendorNumber = fs.VendorNumber
 Order By ps.TotalPurchaseDollars DESC""", conn)

    return vendor_sales_summary


def clean_data(df):
    '''this function will clean the data'''
    
    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float')
    
    # filling missing value with 0
    df.fillna(0, inplace=True)
    
    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    return df


if __name__ == '__main__':
    # creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data.....')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Completed')
