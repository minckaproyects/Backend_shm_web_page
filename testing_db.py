from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import backend.database_connection as db_con

timezone = ZoneInfo('Australia/Melbourne')

date_now = datetime.now(timezone) + timedelta(hours = -10)
year = date_now.year
month = date_now.month
day = date_now.day
hour = date_now.hour
Q2 = datetime(year, month, day, hour, 0, 0)
print("Q2:", Q2)

Q1 = Q2 - timedelta(hours = 1)


print(datetime.now(timezone))
print('Loading data')

accelerations_df, engine = db_con.connection_and_data_retrieving(Q1,Q2)
accelerations_df.to_parquet('data.parquet', index=False)
print(len(accelerations_df))
print(accelerations_df.head())