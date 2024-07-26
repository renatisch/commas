from bots import get_bots_info
from deals import get_deals_info
import dotenv

dotenv.load_dotenv()


def export_data():
    get_bots_info()
    get_deals_info()


export_data()
