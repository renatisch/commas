from bots import get_bots_info
from deals import get_deals_info
import dotenv, datetime, time, schedule

dotenv.load_dotenv()


def ping():
    print("ping")


# def export_data():
#     print(f"Started exporting data at {datetime.datetime.now()}")
#     get_bots_info()
#     get_deals_info()
#     print(f"Finished exporting data at {datetime.datetime.now()}")


schedule.every(10).seconds.do(ping)
while True:
    schedule.run_pending()
    time.sleep(1)
