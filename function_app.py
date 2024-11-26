import logging
import pyodbc
import yfinance as yf
import pandas as pd
import os
import json
import azure.functions as func
from datetime import timedelta, date, datetime, time

app = func.FunctionApp()
CONNECTIONSTRING = os.getenv("SQL_CONNECTION_STRING")


@app.schedule(schedule="0 0 22 * * 1-5", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def FetchData(myTimer: func.TimerRequest) -> None:
    
    stock_symbols = ["MSFT", "AAPL", "NVDA"]; dataStocks = []; conn = None; lastDates = {}
    logging.info("Starting fetchData function.")
    logging.info(f"Stock symbols to process: {stock_symbols}")
    
    try:
        conn = pyodbc.connect(CONNECTIONSTRING)
        logging.info("Database connection established.")
        cursor = conn.cursor()
        counter = 0
        
        for stock in stock_symbols:
            cursor.execute(
                "SELECT TOP 1 DATE FROM StockData WHERE Stock = ? ORDER BY Date DESC",
                (stock,)
            )
            result = cursor.fetchone()
            lastDates[stock] = result[0]
            lastDate = result[0].strftime("%Y-%m-%d")
            startDate = result[0] + timedelta(days=1)
            startDate = startDate.strftime("%Y-%m-%d")
            today = date.today().strftime("%Y-%m-%d")
                        
            if lastDate >= today:
                logging.info(f"Data for {stock} is currently up to date.")
                counter += 1
                continue
            
            logging.info(f"Data for {stock} is not up-to-date. Fetching data from yFinance")
            data = yf.download(stock, start=startDate, interval="1d")
            if not data.empty:
                data['Stock'] = stock
                dataStocks.append(data)
                logging.info(f"Data for {stock} downloaded.")

        if counter == 3:
            logging.info(f"All stocks are up to date. Exiting Function.")
            return
        
        logging.info(f"Inserting new data into database")
        
        for data in dataStocks:
            ticker = data['Stock'].iloc[0] if 'Stock' in data.columns else 'Unknown'
            for index, row in data.iterrows():
                if index.to_pydatetime().replace(tzinfo=None) > lastDates[ticker]:
                    cursor.execute(
                        "INSERT INTO StockData ([Stock], [Date], [Open], [High], [Low], [Close], [Volume]) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ticker, index.to_pydatetime(), float(row['Open'].iloc[0]), float(row['High'].iloc[0]), float(row['Low'].iloc[0]), float(row['Close'].iloc[0]), int(row['Volume'].iloc[0]))
                    )

        conn.commit()
        logging.info("Data successfully inserted into the database.")
    except Exception as e:
        logging.error(f"Failed to insert data into the database. Error: {e}")
    
    finally:
        if conn:
            conn.close()


@app.sql_trigger(arg_name="trigger",
                 table_name="stockData",
                 connection_string_setting="SQL_CONNECTION_STRING_SETTING")
def getAveragesTrigger(trigger: str) -> None:
    logging.info("getAveragesTrigger function activated.")
    logging.info("SQL Changes: %s", json.loads(trigger))
    conn = None
    SMAresults = {}
    stock_symbols = ["MSFT", "AAPL", "NVDA"]

    try:
        connectionString = os.getenv("SQL_CONNECTION_STRING")
        logging.info("Database connection established.")
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()

        for stock_symbol in stock_symbols:
            logging.info(f"Calculating SMAs for {stock_symbol}")
            query = """
                SELECT TOP 50 [Date], [Close] FROM StockData
                WHERE Stock = ?
                ORDER BY Date DESC
            """
            cursor.execute(query, (stock_symbol,))
            rows = cursor.fetchall()

            most_recent_date = rows[0][0]
            

            data_list = [[row[0], row[1]] for row in rows]
            data = pd.DataFrame(data_list, columns=["Date", "Close"])
            data.set_index("Date", inplace=True)
            data.sort_index(inplace=True)

            SMA10 = float(data['Close'].rolling(window=10).mean().iloc[-1]) if len(data) >= 10 else None
            SMA20 = float(data['Close'].rolling(window=20).mean().iloc[-1]) if len(data) >= 20 else None
            SMA50 = float(data['Close'].rolling(window=50).mean().iloc[-1]) if len(data) >= 50 else None

            SMAresults[stock_symbol] = {
                "10-day SMA": SMA10,
                "20-day SMA": SMA20,
                "50-day SMA": SMA50,
                "LastUpdated": most_recent_date  
            }

        logging.info(f"New SMA Results: {SMAresults}")

        for stock_symbol, smas in SMAresults.items():
            cursor.execute("""
                MERGE INTO SMAResults AS target
                USING (VALUES (?, ?, ?, ?, ?)) AS source (StockSymbol, SMA_10_Day, SMA_20_Day, SMA_50_Day, LastUpdated)
                ON target.StockSymbol = source.StockSymbol
                WHEN MATCHED THEN 
                    UPDATE SET 
                        SMA_10_Day = source.SMA_10_Day,
                        SMA_20_Day = source.SMA_20_Day,
                        SMA_50_Day = source.SMA_50_Day,
                        LastUpdated = source.LastUpdated
                WHEN NOT MATCHED THEN 
                    INSERT (StockSymbol, SMA_10_Day, SMA_20_Day, SMA_50_Day, LastUpdated)
                    VALUES (source.StockSymbol, source.SMA_10_Day, source.SMA_20_Day, source.SMA_50_Day, source.LastUpdated);
            """, (
                stock_symbol,
                smas["10-day SMA"],
                smas["20-day SMA"],
                smas["50-day SMA"],
                smas["LastUpdated"]  
            ))

        conn.commit()
        logging.info("SMA results successfully inserted or updated in the database.")

    except Exception as e:
        logging.error(f"Failed to analyze stock data. Error: {e}")
    
    finally:
        if conn:
            conn.close()



@app.route(route="showResults", auth_level=func.AuthLevel.ANONYMOUS)
def showResults(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting showResults Function.')
    
    conn = None

    try:
        connectionString = os.getenv("SQL_CONNECTION_STRING")
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()
        
        query = "SELECT StockSymbol, SMA_10_Day, SMA_20_Day, SMA_50_Day, LastUpdated FROM SMAResults"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        results = {}
        for row in rows:
            results[row.StockSymbol] = {
                "10-day SMA": row.SMA_10_Day,
                "20-day SMA": row.SMA_20_Day,
                "50-day SMA": row.SMA_50_Day,
                "Last Updated": row.LastUpdated.strftime("%Y-%m-%d")
            }
        logging.info(f"SMA results: {results}")
        
        return func.HttpResponse(    
            json.dumps(results),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Failed to retrieve SMA results. Error: {e}")
        return func.HttpResponse(
            "Failed to retrieve SMA results.",
            status_code=500
        )
    
    finally:
        if conn:
            conn.close()
