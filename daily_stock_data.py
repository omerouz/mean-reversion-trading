from urllib import request
import json
import psycopg2
import pandas as pd

class DailyStockDataDownloader():
    def __init__(self, symbol = "MSFT"):
        self.base_url = "https://www.alphavantage.co/query?datatype=json&"
        self.symbol = symbol
        self.licence = ""

    def target_url(self):
        url = self.base_url + "function=TIME_SERIES_DAILY&outputsize=full&"
        url = url + "apikey=" + self.licence + "&"
        url = url + "symbol=" + self.symbol 
        return url

    def retrieve_json_data(self):
        url = self.target_url()
        response = request.urlopen(url)
        bytes_data = response.read()
        json_data = json.loads(bytes_data)
        time_series_map = json_data.get("Time Series (Daily)")
        return time_series_map

# Make basis request to get data

class DailyStockData():

    def __init__(self, symbol="MSFT"):
        self.symbol = symbol
        self.start_date = None
        self.end_date = None
        self.downloader = DailyStockDataDownloader()

    def connect_db(self):
        connection = psycopg2.connect(user="",
                                      password="",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="")
        return connection        

    def connect_db_execute(self, lines, method = "get"):
        record = None
        try:
            connection = self.connect_db()
            cursor = connection.cursor()
            cursor.execute(lines)

            if method is "get":
                record = cursor.fetchall()

            else:
                connection.commit()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
        
        finally:
            cursor.close()
            connection.close()
            return record

    def database_contains_symbol(self, symbol):
        line = "select 1=(select count(*) from stock_symbols where symbol='" + symbol + "');"
        response = self.connect_db_execute(line)
        return response[0][0]

    def download_and_add_data(self, symbol):
        self.downloader.symbol = symbol
        data = self.downloader.retrieve_json_data()

        # Add to Stock_Symbols Table
        stock_symbols_line = "insert into stock_symbols values ('" + symbol + "');"
        self.connect_db_execute(stock_symbols_line, method="push")

        # Add to "Symbol"_Historical_Data Table
        # 1. Step: Create new table:
        create_db_line = "create table " + symbol.lower() + "_historical_data(date_field date primary key, open float, high float, low float, close float, volume bigint);"
        self.connect_db_execute(create_db_line, method="push")

        connection = self.connect_db()
        cursor = connection.cursor()
        # 2.Step: Add lines to the datatable
        for date, key in data.items():
            date_ = date
            open_ = key["1. open"]
            high_ = key["2. high"]
            low_ = key["3. low"]
            close_ = key["4. close"]
            volume_ = key["5. volume"]
            line = "insert into " + symbol + \
                "_historical_data values ('" + date_ + "', " + open_ + ", " + \
                high_ + ", " + close_ + ", " + low_ + ", " + volume_ + ");"
            cursor.execute(line)
        
        connection.commit()
        cursor.close()
        connection.close()
    
    def get_data(self, symbol):
        line = "select * from " + symbol + "_historical_data;"
        response = self.connect_db_execute(line)
        data_frame = pd.DataFrame(response)
        data_frame.index = data_frame[0]
        data_frame = data_frame.drop(columns=[0])
        data_frame = data_frame.rename(columns={1:"Open", 2:"High", 3:"Low", 4:"Close", 5:"Volume"})
        data_frame.index.name = "Date"
        data_frame.index = pd.to_datetime(data_frame.index)
        return data_frame.iloc[::-1]

    def get_pricing_data(self, symbol="MSFT", start_date="2014-01-01", end_date="2015-01-01"):
        symbol = symbol.upper()
        if not self.database_contains_symbol(symbol):
            self.download_and_add_data(symbol)
        data_frame = self.get_data(symbol)
        return data_frame[start_date:end_date]
