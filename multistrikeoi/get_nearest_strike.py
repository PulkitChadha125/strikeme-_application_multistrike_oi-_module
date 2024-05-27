import pandas as pd
from django.db import connection
from multistrikeoi import utlis as u


class GetNearestStrike:
    def combined_oi_calculation(
        symbol,
        instrument,
        strikeprice,
        option_type,
        expiery,
        start_date="2023-08-18",
        end_date="2023-08-19",
    ):
        engine = connection
        df = pd.read_sql_query(
            f"""
            SELECT symbol, expiry_date, strike_price, created_at, ticker, instrument_name, option_type, open_interest FROM indiacharts.fno_prices fp 
            WHERE created_at > '{start_date}' AND created_at < '{end_date}'
            AND symbol = '{symbol}' AND instrument_name = '{instrument}' AND strike_price = {strikeprice}
            AND option_type = '{option_type}'
            ORDER BY created_at ASC
            """,
            con=engine,
        )

        df["created_at_ist"] = df["created_at"].apply(u.utils.convert_utc_to_ist)
        df["created_at_ist"] = pd.to_datetime(df["created_at_ist"])
        df["rounded_created_at_ist"] = df["created_at_ist"].apply(
            u.utils.round_to_nearest_5_minutes
        )

        # Group by rounded_created_at_ist and calculate the sum of open_interest
        oi_sum_df = (
            df.groupby("rounded_created_at_ist")["open_interest"].sum().reset_index()
        )
        df["rounded_created_at_ist"] = pd.to_datetime(df["rounded_created_at_ist"])
        oi_sum_df["rounded_created_at_ist"] = pd.to_datetime(
            oi_sum_df["rounded_created_at_ist"]
        )

        # Merge the DataFrames on 'rounded_created_at_ist'
        merged_df = pd.merge(df, oi_sum_df, on="rounded_created_at_ist", how="left")
        merged_df.rename(columns={"open_interest_y": "Combined OI"}, inplace=True)
        merged_df["expiry_date"] = merged_df["expiry_date"].astype(str)
        merged_df = merged_df[merged_df["expiry_date"] == expiery]
        return merged_df

    def get_historical_eod_data(
        symbol, instrument, strikeprice, option_type, expiery, created_at="2023-08-17"
    ):
        engine = connection

        df = pd.read_sql_query(
            f"""select symbol,expiry_date,strike_price,created_at,ticker, instrument_name,option_type,open_interest from indiacharts.fno_price_eod fpe 
            WHERE created_at ='{created_at}'
            AND symbol = '{symbol}' AND instrument_name = '{instrument}' AND strike_price = {strikeprice}
            AND option_type = '{option_type}' 
            ORDER BY created_at ASC
                               """,
            con=engine,
        )

        # Calculate the sum of open_interest and add it as a new column
        combined_oi_eod = df["open_interest"].sum()
        df["Combined OI EOD"] = combined_oi_eod
        return df

    def get_nearest_strike(symbol, monthly_exp, multiplier, selected_exp, instrument):
        engine = connection
        df = pd.read_sql_query(
            # isme date current date se jada honi chaiee bs
            f"""select * from indiacharts.current_fno_price cfp
                    where instrument_name = 'FUTIDX' and symbol in ('{symbol}')
                    and created_at > '2023-08-18'
                    order by created_at desc
                    """, con=engine)

        df['expiry_date'] = df['expiry_date'].astype(str)
        df = df[df['expiry_date'] == monthly_exp]
        ltp = df['last_traded_price'].astype(int)
        print(ltp)
        u.utils.custom_round(ltp, symbol)
        print(u.utils.custom_round(ltp, symbol))

        if multiplier == 1:
            if symbol == "NIFTY":
                strike =u.utils.custom_round(ltp, symbol)
                call_strike = strike
                put_strike = strike
                u.utils.User_Selected_Strike(symbol=symbol, instrument=instrument, strikeprice=call_strike, option_type="CE",
                                     expiery=selected_exp)
                u.utils.User_Selected_Strike(symbol=symbol, instrument=instrument, strikeprice=put_strike, option_type="PE",
                                     expiery=selected_exp)
            if symbol == "BANKNIFTY":
                strike = u.utils.custom_round(ltp, symbol)
                call_strike = strike
                put_strike = strike
                u.utils.User_Selected_Strike(symbol=symbol, instrument=instrument, strikeprice=call_strike, option_type="CE",
                                     expiery=selected_exp)
                u.utils.User_Selected_Strike(symbol=symbol, instrument=instrument, strikeprice=put_strike, option_type="PE",
                                     expiery=selected_exp)
