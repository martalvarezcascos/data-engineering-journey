from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_csv(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    if df.empty:
        logger.warning("Extracted batch is empty")
    else:
        logger.info(f"Extracted {len(df)} rows")
    return df

