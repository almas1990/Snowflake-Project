import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.point import Point
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time
import dask.dataframe as dd

start_time = time.time()

geolocator = Nominatim(user_agent="otodomprojectanalysis")

engine = create_engine(
    URL(
        account="puwyobp-cv85592",
        user="elearn250",
        password="Le@rn123",
        database="demo",
        schema="public",
        warehouse="demo_wh",
    )
)

with engine.connect() as conn:
    try:
        query = """ SELECT RN, concat(latitude,',',longitude) as LOCATION
                    FROM (SELECT RN
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,4)+1) AS LATITUDE 
                            , SUBSTR(location, REGEXP_INSTR(location,' ',1,1)+1, (REGEXP_INSTR(location,' ',1,2) - REGEXP_INSTR(location,' ',1,1) - 1) ) AS LONGITUDE
                        FROM otodom_data_short_flatten WHERE rn between 1 and 100
                        ORDER BY rn  ) """
        print("--- %s seconds ---" % (time.time() - start_time))

        df = pd.read_sql(query, conn)

        df.columns = map(lambda x: str(x).upper(), df.columns)

        ddf = dd.from_pandas(df, npartitions=1)
        print(ddf.head(5, npartitions=-1))

        delay_between_requests = 2

        def geocode_with_delay(row):
            location = row["LOCATION"]
            address = geolocator.reverse(location).raw["address"]

            return address

        ddf["ADDRESS"] = ddf.apply(geocode_with_delay, axis=1, meta=(None, "str"))

        print("--- %s seconds ---" % (time.time() - start_time))

        pandas_df = ddf.compute()
        print(pandas_df.head())
        print("--- %s seconds ---" % (time.time() - start_time))

        pandas_df.to_sql(
            "otodom_data_flatten_address",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=16000,
            method=pd_writer,
        )
    except Exception as e:
        print("--- Error --- ", e)
    finally:
        conn.close()
engine.dispose()

print("--- %s seconds ---" % (time.time() - start_time))
