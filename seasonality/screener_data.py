import pandas as pd
from django.db import connection


class ScreenerData:
    def get_data(
        date,
        indices_id,
        securities,
        time_period,
        exam_period=10,
        all=False,
        fno=False,
    ):
        engine = connection

        base_query = f"""
        select * from indiacharts.seasonality_screener ss
                    where ss.date = '{date}'
                    and time_period = {time_period} and exam_period = {exam_period}
                    and trade_count >= 5
        """

        fno_query = """ss.security_code in (select security_code from indiacharts.fno_stocks f where f.deleted=False)"""

        indices_query = f"""ss.security_code in 
        (select security_code from indiacharts.indices_stocks is2 where is2.indices_id in ({"'" +  ','.join(indices_id).replace(",", "','") + "'"}))"""

        securities_query = f"""ss.security_code in ({"'" +  ','.join(securities).replace(",", "','") + "'"})"""
        all_query = f"""ss.security_code in (select security_code from indiacharts.indices i where i.ic_active = true)"""

        query = base_query

        if (
            all is True
            and fno is False
            and len(indices_id) <= 0
            and len(indices_id) <= 0
        ):
            query = query + " " + "and" + " " + all_query

        if (
            all is True
            and fno is True
            and len(indices_id) <= 0
            and len(securities) <= 0
        ):
            query = query + " " + "and" + " " + f"({all_query} or {fno_query})"

        if (
            len(indices_id) > 0
            and fno is False
            and all is False
            and len(securities) < 0
        ):
            query = query + " " + "and" + " " + indices_query

        if (
            len(securities) > 0
            and fno is False
            and all is False
            and len(indices_id) < 0
        ):
            query = query + " " + "and" + " " + securities_query

        if (
            len(securities) > 0
            and fno is True
            and all is False
            and len(indices_id) <= 0
        ):
            query = query + " " + "and" + " " + f"({securities_query} or {fno_query})"

        if (
            len(securities) <= 0
            and fno is True
            and all is False
            and len(indices_id) > 0
        ):
            query = query + " " + "and" + " " + f"({indices_query} or {fno_query})"

        if len(securities) > 0 and fno is True and all is False and len(indices_id) > 0:
            query = (
                query
                + " "
                + "and"
                + " "
                + f"({indices_query} or {fno_query} or {securities_query})"
            )

        if (
            len(securities) <= 0
            and fno is True
            and all is False
            and len(indices_id) <= 0
        ):
            query = query + " " + "and" + " " + fno_query

        if (
            len(securities) > 0
            and len(indices_id) > 0
            and fno is False
            and all is False
        ):
            query = (
                query
                + " "
                + "and"
                + "("
                + securities_query
                + " "
                + "or"
                + " "
                + indices_query
                + ")"
            )
        if (
            len(securities) <= 0
            and len(indices_id) > 0
            and fno is False
            and all is False
        ):
            query = query + " " + "and" + " " + indices_query

        if (
            len(securities) > 0
            and len(indices_id) <= 0
            and fno is False
            and all is False
        ):
            query = query + " " + "and" + " " + securities_query

        # print(len(securities), len(indices_id), fno, all)
        print(query)

        df = pd.read_sql_query(query, con=engine)

        return df
