import logging
from pathlib import Path

# Configure logging for the application
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("analysis_pension_ceilings")

PROJECT_PATH = Path(__file__).parents[2].absolute()
SOURCE_PATH = PROJECT_PATH / "src"
APP_PATH = SOURCE_PATH / "analysis_pension_ceilings"

STATIC_PATH = APP_PATH / "static"
DATA_PATH = PROJECT_PATH / "data"
