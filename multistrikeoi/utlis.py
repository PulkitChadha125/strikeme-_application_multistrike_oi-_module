import pytz
import os
from sqlalchemy import create_engine
import pandas as pd
import pytz
from multistrikeoi import user_selected_price as cal
from datetime import datetime,timedelta



class utils:
    def round_to_nearest_5_minutes(ist_time):
        # Calculate the remainder of minutes when divided by 5
        minutes_remainder = ist_time.minute % 5

        # Subtract the remainder from the current minutes to round to the nearest 5 minutes
        rounded_ist_time = ist_time - timedelta(minutes=minutes_remainder)

        # Set seconds to 00
        rounded_ist_time = rounded_ist_time.replace(second=0)

        # Format the time with the desired format using str.format()
        formatted_time = "{}-{:02d}-{:02d} {}:{}:{}".format(
            rounded_ist_time.year,
            rounded_ist_time.month,
            rounded_ist_time.day,
            rounded_ist_time.hour,
            rounded_ist_time.minute,
            rounded_ist_time.second,
        )
        return formatted_time

    def convert_utc_to_ist(utc_time):
        utc_time_str = utc_time.strftime("%Y-%m-%d %H:%M:%S")
        utc_time = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
        utc_time = pytz.utc.localize(utc_time)
        ist_time = utc_time.astimezone(pytz.timezone("Asia/Kolkata"))
        return ist_time.strftime("%Y-%m-%d %H:%M:%S")

    def custom_round(prices, symbol):
        rounded_prices = []

        for price in prices:
            if symbol == "NIFTY":
                last_two_digits = price % 100
                if last_two_digits < 25:
                    rounded_price = (price // 100) * 100
                elif last_two_digits < 75:
                    rounded_price = (price // 100) * 100 + 50
                else:
                    rounded_price = (price // 100 + 1) * 100
            elif symbol == "BANKNIFTY":
                last_two_digits = price % 100
                if last_two_digits < 50:
                    rounded_price = (price // 100) * 100
                else:
                    rounded_price = (price // 100 + 1) * 100
            else:
                rounded_price = price  # Handle the "else" case

            rounded_prices.append(rounded_price)

        return rounded_prices[0]

    def User_Selected_Strike(symbol, instrument, strikeprice, option_type,expiery):
        df_selected_strike = cal.UserSelectedPrice.combined_oi_calculation(symbol, instrument=instrument, strikeprice=strikeprice, option_type=option_type,
                                expiery=expiery)
        df_eod_data = cal.UserSelectedPrice.get_historical_eod_data(symbol, instrument=instrument, strikeprice=strikeprice, option_type=option_type,
                                expiery=expiery)

        csv_filename = f'{symbol}_{strikeprice}_{option_type}_{expiery}.csv'
        eod_csv_filename = f'{symbol}_{strikeprice}_{option_type}_{expiery}_eod.csv'

        df_selected_strike = pd.read_csv(csv_filename)
        df_eod_data = pd.read_csv(eod_csv_filename)

        # Merge based on 'expiry_date'
        merged_df = df_selected_strike.merge(df_eod_data[['expiry_date', 'open_interest', 'Combined OI EOD']],
                                             on='expiry_date', how='left')
        merged_df.rename(columns={"open_interest": "Yesterday Eod OI"}, inplace=True)
        merged_df["Change In Oi"] = merged_df["open_interest_x"] - merged_df["Yesterday Eod OI"]
        merged_df["Change In Combined Oi"] = merged_df["Combined OI"] - merged_df["Combined OI EOD"]

        merged_df_json = merged_df.to_json(orient='records')

        return merged_df_json
        # print(merged_df_json)

        # merged_df.to_csv(f'{symbol}_{strikeprice}_{option_type}_{expiery}.csv', index=False)






