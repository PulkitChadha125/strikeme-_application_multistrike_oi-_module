import numpy as np
import pandas as pd
from datetime import datetime
from datetime import time
from datetime import timedelta
from dateutil.relativedelta import relativedelta


class Symbol_Seasonality_Individual:
    """
    This class generates a seasonality instance for a given security with all its metrics.
    """

    def __init__(self, symbol, price_data):
        """
        Symbol must be the name of the symbol. Price data must be the ohlcv data with date column as datetime/pd.Timestamp objects.
        Volume can be zero.
        """

        # INITIALISE KEY VARIABLES
        self.symbol = symbol
        self.price_data = price_data.copy()
        self.price_data = self.price_data[["datetime", "close"]].copy()
        self.custom_backtest = None
        self.custom_backtest_metrics = None
        self.custom_window = None

        # DEFAULT VALUES FOR INITIAL HORIZON
        self.horizon_end = self.price_data.iloc[-1]["datetime"]
        self.horizon_start = self.horizon_end - relativedelta(years=10)
        self.earliest = self.price_data.iloc[0]["datetime"]
        self.latest = self.price_data.iloc[-1]["datetime"]

        # GET RETURNS AND DETRENDED RETURNS DATA
        self.returns_data = self.get_returns_data(df=self.price_data)
        self.detrended_returns_data = self.get_detrended_returns_data(
            df=self.returns_data
        )

        # RUN EXECUTION SEQUENCE
        self.exec_sequence()

    def update_horizon(self, start, end):
        """
        Updates the data horizon for a symbol class. Also runs self.exec_sequence to update values.
        """
        self.horizon_start = max(pd.Timestamp(start), self.earliest)
        self.horizon_end = min(pd.Timestamp(end), self.latest)

        if self.horizon_start >= self.horizon_end:
            raise Exception("Invalid input,. 'start' cannot be greater than 'end'.")

        self.exec_sequence()

    def get_returns_data(self, df):
        """
        Gets the percent change of returns for close of each candle.
        """
        df = df.copy()
        # df['pct_change'] = (df['close'] - df['close'].shift(1))*100/df['close'].shift(1)    # ARRITHMETIC %
        df["pct_change"] = (
            np.log(df.close) - np.log(df.close.shift(1))
        ) * 100  # LOGARITHMIC %
        df = df[["datetime", "pct_change"]].copy()
        df.dropna(subset=["pct_change"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_detrended_returns_data(self, df):
        """
        Detrends price data by subtracting mean candle return
        """
        df = df.copy()
        mean_ret = df["pct_change"].mean()
        df["pct_change"] -= mean_ret
        return df

    def get_annual_seasonality(self, df):
        """
        Obtains seasonality for a given dataframe.
        """
        df = df.copy()
        df = df[(df['datetime'] >= self.horizon_start) & (df['datetime'] <= self.horizon_end)].copy()
        df.reset_index(drop=True)
        df.insert(1,'day_month', [x.strftime('%d-%b') for x in df['datetime']])
        df.drop(columns=['datetime'], inplace=True)
        df = df.groupby(by=['day_month']).mean()
        df.reset_index(inplace=True)
        df.insert(0,'plot_date', [datetime.strptime(x+'-2000','%d-%b-%Y') for x in df['day_month']])
        df.sort_values(by='plot_date', ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['cumulative_returns'] = df['pct_change'].cumsum()
        df['cumulative_returns'] += 100
        return df

    def get_max_performance_window(self, eval_days, df, direction):
        """
        Finds the best performance period for given days in a seasonality df.
        """
        df = df.copy()
        df.reset_index(drop=True, inplace=True)
        results = {"ret": [], "start": [], "end": []}

        for i in range(0, len(df)):
            start = df.iloc[i]["plot_date"]
            end = start + timedelta(days=eval_days)
            t_df = df[(df["plot_date"] >= start) & (df["plot_date"] <= end)].copy()
            t_df.reset_index(drop=True, inplace=True)
            ret = (
                t_df.iloc[-1]["cumulative_returns"] - t_df.iloc[0]["cumulative_returns"]
            )
            results["ret"].append(ret)
            results["start"].append(start)
            results["end"].append(end)

        results = pd.DataFrame(results)

        if direction == "UP":
            results.sort_values(by="ret", ascending=False, inplace=True)

        elif direction == "DOWN":
            results.sort_values(by="ret", ascending=True, inplace=True)

        results.reset_index(drop=True, inplace=True)
        start = results.iloc[0]["start"]
        end = results.iloc[0]["end"]
        ret = results.iloc[0]["ret"]
        return {
            "start": {"month": start.month, "day": start.day},
            "end": {"month": end.month, "day": end.day},
            "ret": ret,
        }

    def get_max_performance_window_in_range(
        self, range_start, range_end, df, direction
    ):
        """
        Finds the best performance window in a potential range of days.
        """
        df = df.copy()
        results = {"ret": [], "start": [], "end": [], "window": []}
        for eval_days in range(range_start, range_end + 1):
            iter_result = self.get_max_performance_window(
                eval_days=eval_days, df=df, direction=direction
            )
            results["ret"].append(iter_result["ret"])
            results["start"].append(iter_result["start"])
            results["end"].append(iter_result["end"])
            results["window"].append(eval_days)

        results = pd.DataFrame(results)

        if direction == "UP":
            results.sort_values(by="ret", ascending=False, inplace=True)

        elif direction == "DOWN":
            results.sort_values(by="ret", ascending=True, inplace=True)

        results.reset_index(drop=True, inplace=True)
        start = results.iloc[0]["start"]
        end = results.iloc[0]["end"]
        ret = results.iloc[0]["ret"]
        window = results.iloc[0]["window"]
        return {"start": start, "end": end, "ret": ret, "window": window}

    def format_to_window(self, start_month, start_day, end_month, end_day):
        """
        Builds an evaluation window to acceptable format. Feb 29 is adjusted to Feb 28.
        """
        if start_month == 2 and start_day == 29:
            start_day = 28

        if end_month == 2 and end_day == 29:
            end_day = 28

        return {
            "start": {"month": start_month, "day": start_day},
            "end": {"month": end_month, "day": end_day},
        }

    def get_backtest_batches(self, start, end, series):
        """
        Builds list of batches for backtesting. Make sure this does not receive feb 29!
        """
        batches = []
        series_start = series[0]
        series_end = series[-1]

        for i in range(series_start.year, series_end.year + 1):
            batch_start = pd.Timestamp(datetime(i, start["month"], start["day"]))
            batch_end = pd.Timestamp(datetime(i, end["month"], end["day"]))

            if batch_start >= batch_end:
                batch_end += relativedelta(years=1)

            pre_diff = (batch_end - batch_start).days

            while True:
                if batch_start in series or batch_start > series_end:
                    break

                batch_start += timedelta(days=1)

            while True:
                if batch_end in series or batch_end > series_end:
                    break

                batch_end += timedelta(days=1)

            post_diff = (batch_end - batch_start).days

            if (
                batch_end <= series_end
                and batch_start != batch_end
                and post_diff >= 0.75 * pre_diff
            ):
                batch = {"start": batch_start, "end": batch_end}
                batches.append(batch)

        return batches

    def get_trade_metrics(self, df):
        """
        Returns trade df for price data.
        """
        df = df.copy()
        start_date = df.iloc[0]["datetime"]
        end_date = df.iloc[-1]["datetime"]
        start_price = df.iloc[0]["close"]
        end_price = df.iloc[-1]["close"]
        max_up = df["close"].max()
        max_dwn = df["close"].min()
        profit = end_price - start_price
        profit_pct = round((profit * 100) / start_price, 2)
        max_rise = round(((max_up - start_price) * 100) / start_price, 2)
        max_drop = -round(((start_price - max_dwn) * 100) / start_price, 2)
        columns = [
            "Start date",
            "Start price",
            "End date",
            "End price",
            "Profit",
            "Profit %",
            "Max rise",
            "Max drop",
        ]
        row_df = pd.DataFrame(
            [
                [
                    start_date,
                    start_price,
                    end_date,
                    end_price,
                    profit,
                    profit_pct,
                    max_rise,
                    max_drop,
                ]
            ],
            columns=columns,
        )
        return row_df

    def build_backtest(self, start_month, start_day, end_month, end_day, df):
        """
        Builds backtest from price data for performance during a date range for all the years.
        """
        window_info = self.format_to_window(
            start_month=start_month,
            start_day=start_day,
            end_month=end_month,
            end_day=end_day,
        )
        start = window_info["start"]
        end = window_info["end"]
        df = df.copy()
        df = df[
            (df["datetime"] >= self.horizon_start)
            & (df["datetime"] <= self.horizon_end)
        ].copy()
        df.reset_index(drop=True, inplace=True)
        batches = self.get_backtest_batches(
            start=start, end=end, series=df["datetime"].tolist()
        )
        df_list = [pd.DataFrame()]

        for batch in batches:
            batch_start = batch["start"]
            batch_end = batch["end"]
            t_df = df[
                (df["datetime"] >= batch_start) & (df["datetime"] <= batch_end)
            ].copy()
            t_df.reset_index(drop=True, inplace=True)
            result = self.get_trade_metrics(df=t_df)
            df_list.append(result)

        backtest = pd.concat(df_list, axis=0)
        backtest['cumulative_profit'] = backtest['Profit'].cumsum()
        backtest.reset_index(drop=True, inplace=True)
        return backtest

    def calculate_streak(self, series):
        """
        Calculates latest streak backwards and return +ve or -ve value based on streak was winning or losing.
        """
        win_streak = 0

        for x in series:
            if x > 0:
                win_streak += 1

            else:
                break

        loss_streak = 0

        for x in series:
            if x < 0:
                loss_streak += 1

            else:
                break

        if win_streak > loss_streak:
            return win_streak

        elif loss_streak > win_streak:
            return -loss_streak

        else:
            return 0

    def get_annualised_return(self, df):
        """
        Returns the annualised return for the backtest.
        """
        df = df.copy()
        df["days_invested"] = [
            (y - x).days for x, y in zip(df["Start date"], df["End date"])
        ]
        total_days = df["days_invested"].sum()
        elems = [(1 + x / 100) for x in df["Profit %"]]
        result = 1

        for x in elems:
            result = x * result

        years = total_days / 365
        ret = ((result ** (1 / years)) - 1) * 100
        return round(ret, 2)

    def get_sharpe(self):
        """
        Calculates Sharpe for a backtest. Needs work if neccessary.
        """
        return None

    def get_backtest_metrics(self, df):
        """
        Generates metrics for a backtest dataframe. Returns a dictionary of metrics. native is for values native to symbol object. screener is for those values needed for screening.
        """
        df = df.copy()

        if len(df) != 0:
            backtest_metrics = {"native": {}, "screener": {}}

            # WIN RATE AND TRADE COUNT SECTION
            total_trades = len(df)
            gains = [x for x in df["Profit %"] if x > 0]
            losses = [x for x in df["Profit %"] if x < 0]
            winner_count = len(gains)
            loser_count = total_trades - winner_count
            win_rate = round((winner_count * 100) / total_trades, 2)

            # RETURNS SECTION
            annualised_return = self.get_annualised_return(df=df)
            average_return = round(df["Profit %"].mean(), 2)
            median_return = round(df["Profit %"].median(), 2)

            # PROFITS SECTIONS
            total_profit = round(df["Profit"].sum(), 2)
            average_profit = round(df["Profit"].mean(), 2)

            # GAINS SECTION
            gain_count = len(gains)
            loss_count = len(losses)

            if len(gains) == 0:
                average_gain = 0
                max_gain = 0

            else:
                average_gain = round(sum(gains) / len(gains), 2)
                max_gain = round(max(gains), 2)

            if len(losses) == 0:
                average_loss = 0
                max_loss = 0

            else:
                average_loss = round(sum(losses) / len(losses), 2)
                max_loss = round(min(losses), 2)

            # MISCELLANEOUS SECTION
            series = df["Profit %"].iloc[::-1].tolist()
            calendar_days = int(
                round(
                    sum(
                        [(y - x).days for x, y in zip(df["Start date"], df["End date"])]
                    )
                    / len(
                        [(y - x).days for x, y in zip(df["Start date"], df["End date"])]
                    ),
                    0,
                )
            )
            trading_days = int(round(calendar_days - ((calendar_days / 7) * 2), 0))
            std_dev = round(df["Profit %"].std(), 2)
            streak = self.calculate_streak(series=series)
            sharpe = self.get_sharpe()

            # SYMBOL NATIVE METRICS
            backtest_metrics["native"]["annualised_return"] = annualised_return
            backtest_metrics["native"]["winning_trades"] = win_rate
            backtest_metrics["native"]["average_return"] = average_return
            backtest_metrics["native"]["median_return"] = median_return
            backtest_metrics["native"]["total_profit"] = total_profit
            backtest_metrics["native"]["average_profit"] = average_profit
            backtest_metrics["native"]["gains_count"] = gain_count
            backtest_metrics["native"]["losses_count"] = loss_count
            backtest_metrics["native"]["average_gain"] = average_gain
            backtest_metrics["native"]["average_loss"] = average_loss
            backtest_metrics["native"]["max_gain"] = max_gain
            backtest_metrics["native"]["max_loss"] = max_loss
            backtest_metrics["native"]["trades"] = total_trades
            backtest_metrics["native"]["current_streak"] = streak
            backtest_metrics["native"]["trading_days"] = trading_days
            backtest_metrics["native"]["calendar_days"] = calendar_days
            backtest_metrics["native"]["sharpe"] = sharpe
            backtest_metrics["native"]["standard_deviation"] = std_dev

            # SCREENER METRICS
            backtest_metrics["screener"]["annualised_return"] = annualised_return
            backtest_metrics["screener"]["average_return"] = average_return
            backtest_metrics["screener"]["median_return"] = median_return
            backtest_metrics["screener"]["max_profit"] = max_gain
            backtest_metrics["screener"]["max_loss"] = max_loss
            backtest_metrics["screener"]["winner_count"] = gain_count
            backtest_metrics["screener"]["trade_count"] = total_trades
            backtest_metrics["screener"]["win_ratio"] = win_rate
            backtest_metrics["screener"]["standard_deviation"] = std_dev
            backtest_metrics["screener"]["sharpe"] = sharpe
            return backtest_metrics

        else:
            backtest_metrics = {"native": {}, "screener": {}}

            # SYMBOL NATIVE METRICS
            backtest_metrics["native"]["annualised_return"] = None
            backtest_metrics["native"]["winning_trades"] = None
            backtest_metrics["native"]["average_return"] = None
            backtest_metrics["native"]["median_return"] = None
            backtest_metrics["native"]["total_profit"] = None
            backtest_metrics["native"]["average_profit"] = None
            backtest_metrics["native"]["gains_count"] = None
            backtest_metrics["native"]["losses_count"] = None
            backtest_metrics["native"]["average_gain"] = None
            backtest_metrics["native"]["average_loss"] = None
            backtest_metrics["native"]["max_gain"] = None
            backtest_metrics["native"]["max_loss"] = None
            backtest_metrics["native"]["trades"] = None
            backtest_metrics["native"]["current_streak"] = None
            backtest_metrics["native"]["trading_days"] = None
            backtest_metrics["native"]["calendar_days"] = None
            backtest_metrics["native"]["sharpe"] = None
            backtest_metrics["native"]["standard_deviation"] = None

            # SCREENER METRICS
            backtest_metrics["screener"]["annualised_return"] = None
            backtest_metrics["screener"]["average_return"] = None
            backtest_metrics["screener"]["median_return"] = None
            backtest_metrics["screener"]["max_profit"] = None
            backtest_metrics["screener"]["max_loss"] = None
            backtest_metrics["screener"]["winner_count"] = None
            backtest_metrics["screener"]["trade_count"] = None
            backtest_metrics["screener"]["win_ratio"] = None
            backtest_metrics["screener"]["standard_deviation"] = None
            backtest_metrics["screener"]["sharpe"] = None
            return backtest_metrics

    def update_custom_backtest(self, start_month, start_day, end_month, end_day):
        """
        Generate a custom backtest.
        """
        self.custom_backtest = self.build_backtest(
            start_month=start_month,
            start_day=start_day,
            end_month=end_month,
            end_day=end_day,
            df=self.price_data,
        )
        self.custom_backtest_metrics = self.get_backtest_metrics(
            df=self.custom_backtest
        )
        self.custom_window = self.format_to_window(
            start_month=start_month,
            start_day=start_day,
            end_month=end_month,
            end_day=end_day,
        )

    def exec_sequence(self):
        """
        Runs the execution sequence to recalculate all results.
        """

        # GET SEASONALITY
        self.annual_seasonality = self.get_annual_seasonality(df=self.returns_data)
        self.detrended_annual_seasonality = self.get_annual_seasonality(
            df=self.detrended_returns_data
        )

        # AUTO-BACKTEST OVER BEST PERFORMANCE WINDOW WITH DIRECTION UP
        self.best_performance_window = self.get_max_performance_window(
            eval_days=30, df=self.annual_seasonality, direction="UP"
        )
        # self.best_performance_window_range = self.get_max_performance_window_in_range(range_start=5, range_end=30, df=self.annual_seasonality, direction='UP')
        start_month = self.best_performance_window["start"]["month"]
        start_day = self.best_performance_window["start"]["day"]
        end_month = self.best_performance_window["end"]["month"]
        end_day = self.best_performance_window["end"]["day"]
        self.best_performance_backtest = self.build_backtest(
            start_month=start_month,
            start_day=start_day,
            end_month=end_month,
            end_day=end_day,
            df=self.price_data,
        )
        self.best_performance_metrics = self.get_backtest_metrics(
            df=self.best_performance_backtest
        )

        # AUTO-BACKTEST OVER BEST PERFORMANCE WINDOW WITH DIRECTION DOWN
        self.worst_performance_window = self.get_max_performance_window(
            eval_days=30, df=self.annual_seasonality, direction="DOWN"
        )
        # self.worst_performance_window_range = self.get_max_performance_window_in_range(range_start=5, range_end=30, df=self.annual_seasonality, direction='DOWN')
        start_month = self.worst_performance_window["start"]["month"]
        start_day = self.worst_performance_window["start"]["day"]
        end_month = self.worst_performance_window["end"]["month"]
        end_day = self.worst_performance_window["end"]["day"]
        self.worst_performance_backtest = self.build_backtest(
            start_month=start_month,
            start_day=start_day,
            end_month=end_month,
            end_day=end_day,
            df=self.price_data,
        )
        self.worst_performance_metrics = self.get_backtest_metrics(
            df=self.worst_performance_backtest
        )

        # UPDATE CUSTOM BACKTEST IF IT HAS ALREADY BEEN GENERATED
        if self.custom_window != None:
            start_month = self.custom_window["start"]["month"]
            start_day = self.custom_window["start"]["day"]
            end_month = self.custom_window["end"]["month"]
            end_day = self.custom_window["end"]["day"]
            self.update_custom_backtest(
                start_month=start_month,
                start_day=start_day,
                end_month=end_month,
                end_day=end_day,
            )
