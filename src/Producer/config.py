import logging
import os
from dotenv import load_dotenv

load_dotenv()

# CONFGURE LOGGING
logging.basicConfig(
    filename = 'LogFile.log',
    filemode = 'w',
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level = logging.INFO,
)

logger = logging.getLogger(__name__)

# API CONFIGURATION and BASE_URL
api_key = os.getenv('RAPIDAPI_KEY')
host_domain = os.getenv('RAPIDAPI_HOST')

url = f"https://{host_domain}/query"


headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": host_domain,
	"Content-Type": "application/json"
}
