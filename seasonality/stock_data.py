from pytz import timezone
from django.db import connection
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import time
from datetime import timedelta


class HistoricalData:
    def ic_historical_data(
        symbol,
        start,
        end,
        tf,
        indices_id,
        securities,
        is_symbol=True,
        is_indices=False,
        all=False,
        fno=False,
        watchlist=False,
        exchange="NSE",
    ):
        engine = connection

        """
        Obtains data from IC DB and returns pandas dataframe.
        'symbol' must be the symbol name.
        'exchange' is the exchange. Currently accepts a default value of 'NSE'.
        'start' and 'end' must be datetime objects.
        'tf' is the timeframe to request end output of data. Currently only supports higher timeframes of 'D', 'W' and 'M'.
        Intraday timeframes are not supported for now.
        """

        def get_tf_data(tf):
            """
            Gets offset related info for a requested timeframe as well as the type of series to pull.
            If choosing "['D','W','M']", then the end result will request a 'daily' series. Else intraday.
            """

            tf_list = ["D", "W", "M"]

            if tf in tf_list:
                str_tf = tf
                tf_type = "daily"
                return {"str_tf": str_tf, "tf_type": tf_type}

            elif tf % 5 == 0 and 1440 % tf == 0:
                str_tf = str(tf) + "min"
                tf_type = "intraday"
                return {"str_tf": str_tf, "tf_type": tf_type}

            else:
                raise Exception(
                    "Invalid Requested Timeframe. Timeframes must be [D, W, M] or a multiple of 5 min."
                )

        def ohlcv_resample(df, tf):
            """
            Handles resampling for higher timeframes like 'W' and 'M'. Must be fed a daily data series. (Not Intraday)
            """

            def tf_switch(c_stamp, p_stamp, freq):
                """
                Checks for different triggers of timeframes.
                """

                if freq == "D":
                    if c_stamp.day != p_stamp.day:
                        return True

                if freq == "W":
                    if (c_stamp.weekday() == 0) or (
                        c_stamp.weekday() > 0
                        and p_stamp.weekday() > 0
                        and c_stamp.weekday() < p_stamp.weekday()
                    ):
                        return True

                if freq == "M":
                    if c_stamp.month != p_stamp.month:
                        return True

            col_list = [
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "symbol",
                "name",
                "type",
            ]
            cndl_dict = {
                "datetime": [],
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": [],
                "symbol": [],
                "name": [],
                "type": [],
            }
            d = float("nan")
            o = float("nan")
            h = float("nan")
            l = float("nan")
            c = float("nan")
            v = float("nan")
            s = str("symbol")
            n = str("name")
            t = str("type")

            for index, row in df.iterrows():
                if index == 0:
                    d = row["datetime"]
                    o = row["open"]
                    h = row["high"]
                    l = row["low"]
                    c = row["close"]
                    v = row["volume"]
                    s = row["symbol"]
                    n = row["name"]
                    t = row["type"]
                    continue

                c_stamp = row["datetime"]
                p_stamp = df.at[index - 1, "datetime"]
                sw_flg = tf_switch(c_stamp=c_stamp, p_stamp=p_stamp, freq=tf)

                if sw_flg:
                    cndl_dict["datetime"].append(d)
                    cndl_dict["open"].append(o)
                    cndl_dict["high"].append(h)
                    cndl_dict["low"].append(l)
                    cndl_dict["close"].append(c)
                    cndl_dict["volume"].append(v)
                    cndl_dict["symbol"].append(s)
                    cndl_dict["name"].append(n)
                    cndl_dict["type"].append(t)
                    d = row["datetime"]
                    o = row["open"]
                    h = row["high"]
                    l = row["low"]
                    c = row["close"]
                    v = row["volume"]
                    s = row["symbol"]
                    n = row["name"]
                    t = row["type"]

                else:
                    h = max(h, row["high"])
                    l = min(l, row["low"])
                    c = row["close"]
                    v += row["volume"]
                    s = row["symbol"]
                    n = row["name"]
                    t = row["type"]

            cndl_dict["datetime"].append(d)
            cndl_dict["open"].append(o)
            cndl_dict["high"].append(h)
            cndl_dict["low"].append(l)
            cndl_dict["close"].append(c)
            cndl_dict["volume"].append(v)
            cndl_dict["symbol"].append(s)
            cndl_dict["name"].append(n)
            cndl_dict["type"].append(t)
            df = pd.DataFrame(cndl_dict)
            return df

        def ic_db_eod_stock_data(symbol, exchange, start, end, engine):
            """
            Fetches the corresponding EOD data from sql db for india charts.
            """
            if is_symbol:
                df = pd.read_sql_query(
                    f"""
                        select spe.ticker, spe.created_at, spe.symbol, spe.security_code, spe.open_price, spe.high_price, spe.low_price, spe.close_price, spe.total_trade_quantity,
                        i.name ,
                        case
                        	when s.security_type_code = 26 then 'index'
                        	when s.security_type_code = 5 then 'stock'
                        end as type
                        from indiacharts.stock_price_eod spe
                        join indiacharts.stocks s on s.security_code = spe.security_code 
                        left join indiacharts.indices i on i.security_code = spe.security_code
                        where spe.created_at >= '{start}' and spe.created_at <= '{end}' 
                        and spe.symbol = '{symbol}'
                        order by created_at asc
                        """,
                    con=engine,
                )
            elif is_indices:
                df = pd.read_sql_query(
                    f"""select spe.ticker, spe.created_at, spe.symbol,spe.security_code, spe.open_price, spe.high_price, spe.low_price, spe.close_price, spe.total_trade_quantity,
                        i.name ,
                        case
                        	when s.security_type_code = 26 then 'index'
                        	when s.security_type_code = 5 then 'stock'
                        end as type
                        from indiacharts.stock_price_eod spe
                        join indiacharts.stocks s on s.security_code = spe.security_code 
                        join indiacharts.indices i on i.security_code = spe.security_code
                        where spe.created_at >= '{start}' and spe.created_at <= '{end}' 
                        order by created_at asc""",
                    con=engine,
                )
            elif all:
                df = pd.read_sql_query(
                    f"""select spe.ticker, spe.created_at, spe.symbol,spe.security_code, spe.open_price, spe.high_price, spe.low_price, spe.close_price, spe.total_trade_quantity,
                        i.name ,
                        case
                        	when s.security_type_code = 26 then 'index'
                        	when s.security_type_code = 5 then 'stock'
                        end as type
                        from indiacharts.stock_price_eod spe
                        join indiacharts.stocks s on s.security_code = spe.security_code 
                        left join indiacharts.indices i on i.security_code = spe.security_code
                        where spe.created_at >= '{start}' and spe.created_at <= '{end}'
                        and (spe.ticker in (select ticker_name from indiacharts.indices_stocks is2 where is2.indices_id in {indices_id}
                        or i.ic_active = true or spe.ticker in (select distinct ticker from indiacharts.fno_stocks f where f.deleted = False)
                        or security_code in {securities}))
                        order by created_at asc""",
                    con=engine,
                )
            elif fno:
                df = pd.read_sql_query(
                    f"""select spe.ticker, spe.created_at, spe.symbol,spe.security_code, spe.open_price, spe.high_price, spe.low_price, spe.close_price, spe.total_trade_quantity,
                        null as name ,
                        case
                        	when s.security_type_code = 26 then 'index'
                        	when s.security_type_code = 5 then 'stock'
                        end as type
                        from indiacharts.stock_price_eod spe
                        join indiacharts.stocks s on s.security_code = spe.security_code 
                        where spe.created_at >= '{start}' and spe.created_at <= '{end}'
                        and spe.ticker in (select distinct ticker from indiacharts.fno_stocks f where f.deleted = False)
                        order by spe.created_at asc""",
                    con=engine,
                )
            elif watchlist:
                df = pd.read_sql_query(
                    f"""select spe.ticker, spe.created_at, spe.symbol,spe.security_code, spe.open_price, spe.high_price, spe.low_price, spe.close_price, spe.total_trade_quantity,
                        i.name ,
                        case
                        	when s.security_type_code = 26 then 'index'
                        	when s.security_type_code = 5 then 'stock'
                        end as type
                        from indiacharts.stock_price_eod spe
                        join indiacharts.stocks s on s.security_code = spe.security_code 
                        left join indiacharts.indices i on i.security_code = spe.security_code
                        where spe.created_at >= '{start}' and spe.created_at <= '{end}'
                        and spe.security_code in {securities}
                        order by spe.created_at asc""",
                    con=engine,
                )
            else:
                df = pd.read_sql_query(
                    f"""
                        select spe.ticker, spe.created_at, spe.symbol,spe.security_code, spe.open_price, spe.high_price, spe.low_price, spe.close_price, spe.total_trade_quantity,
                        i.name ,
                        case
                        	when s.security_type_code = 26 then 'index'
                        	when s.security_type_code = 5 then 'stock'
                        end as type
                        from indiacharts.stock_price_eod spe
                        join indiacharts.stocks s on s.security_code = spe.security_code 
                        left join indiacharts.indices i on i.security_code = spe.security_code
                        where spe.created_at >= '{start}' and spe.created_at <= '{end}'
                        and s.exchange = '{exchange}' 
                        and spe.symbol in (select symbol from indiacharts.indices_stocks is2 where is2.indices_id in {indices_id})
                        order by spe.created_at asc
                        """,
                    con=engine,
                )

            df = df[
                [
                    "created_at",
                    "symbol",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "total_trade_quantity",
                    "name",
                    "type",
                ]
            ].copy()
            df.columns = [
                "datetime",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "name",
                "type",
            ]
            df["datetime"] = [datetime.combine(x, time(0, 0)) for x in df["datetime"]]
            return df

        def ic_db_intraday_stock_data(symbol, exchange, start, end, engine):
            """
            Fetches the corresponding Intraday data from sql db for india charts.
            """

            def naive_tz_adjustment(ts, current_tz, to_tz):
                """
                Format Naive TZ object to localised and then TZ adjusted object.
                """
                aware_ts = current_tz.localize(ts)
                tz_adjusted_ts = aware_ts.astimezone(to_tz)
                return tz_adjusted_ts

            def tz_adjustment(ts, to_tz):
                """
                Format TZ aware object to TZ adjusted object.
                """
                tz_adjusted_ts = ts.astimezone(to_tz)
                return tz_adjusted_ts

            # PULL RAW DATA FROM DB
            start = naive_tz_adjustment(
                ts=start, current_tz=timezone("Asia/Kolkata"), to_tz=timezone("UTC")
            )
            end = naive_tz_adjustment(
                ts=end, current_tz=timezone("Asia/Kolkata"), to_tz=timezone("UTC")
            )
            df = pd.read_sql_query(
                f"""
                select ticker, created_at, symbol,security_code, open_price, high_price, low_price, close_price, traded_quantity
                from indiacharts.stock_prices
                where created_at >= '{start}' and created_at <= '{end}'
                and ticker in (select distinct ticker from indiacharts.stocks where exchange = '{exchange}' and symbol = '{symbol}')
                order by created_at asc
                """,
                con=engine,
            )

            # PROCESS OUTPUTS AS PER STANDARDIZED FORMAT
            df = df[
                [
                    "created_at",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "traded_quantity",
                ]
            ].copy()
            df.columns = ["datetime", "open", "high", "low", "close", "volume"]
            df["datetime"] = [
                x.replace(second=0) - timedelta(minutes=4) for x in df["datetime"]
            ]
            df["datetime"] = [
                tz_adjustment(ts=x, to_tz=timezone("Asia/Kolkata")).replace(tzinfo=None)
                for x in df["datetime"]
            ]
            df["time"] = [x.time() for x in df["datetime"]]
            df["wrong"] = [1 if x.minute % 5 != 0 else 0 for x in df["time"]]
            df = df[(df["time"] >= time(9, 15)) & (df["time"] < time(15, 30))].copy()
            df = df[df["wrong"] == 0].copy()
            df.drop(columns=["time", "wrong"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df

        tf_data = get_tf_data(tf=tf)
        str_tf = tf_data["str_tf"]
        tf_type = tf_data["tf_type"]
        ohlc = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }

        if tf_type == "intraday":
            df = ic_db_intraday_stock_data(
                symbol=symbol, exchange=exchange, start=start, end=end, engine=engine
            )
            df.set_index("datetime", inplace=True)
            df = df.resample(str_tf, offset="555min").apply(ohlc)
            df.dropna(inplace=True)
            df.reset_index(inplace=True)
            return df

        if tf_type == "daily":
            df = ic_db_eod_stock_data(
                symbol=symbol, exchange=exchange, start=start, end=end, engine=engine
            )
            df = ohlcv_resample(df=df, tf=str_tf)
            return df
