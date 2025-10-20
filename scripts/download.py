import re
import json 
from datetime import datetime, timedelta
from argparse import ArgumentParser

with open("config/data_config.json", "r") as f:
    CONFIG_LOADER = json.load(f)
def parse_args():
    parser = ArgumentParser(description="Download data based on configuration.")
    parser.add_argument(
        "--download_name",
        type=str,
        required=True,
        choices=list(CONFIG_LOADER.keys()),
        help="Name of the download configuration to use.",
    )
    
    parser.add_argument(
        "--max_download",
        type=int,
        default=-1,
        help="Maximum number of files to download. Use -1 for no limit.",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="2025-10-16",
        help="Start date for the download in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date for the download in YYYY-MM-DD format.",
    )
        
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    download_name = args.download_name
    START_DATE = datetime.strptime(args.start_date, "%Y-%m-%d")
    END_DATE = datetime.strptime(args.end_date, "%Y-%m-%d")
    days = (END_DATE - START_DATE).days + 1
    dates = [(START_DATE + timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]

    if download_name not in CONFIG_LOADER:
        raise ValueError(f"Download name '{download_name}' not found in configuration.")
    
    config = CONFIG_LOADER[download_name]
    if config.get('type') == "dila_folder":
        from src.download.dila import DilaDownloader
        if download_name == "legi":
            pattern_dates = [f"LEGI_{date}" for date in dates] + [f"Freemium_legi_global_{date}" for date in dates]
            patterns = [re.compile(fr"{pattern}-[0-9]{{6}}\.tar\.gz") for pattern in pattern_dates]
            regular_pattern = re.compile(r"(LEGI|Freemium_legi_global)_\d{8}-\d{6}\.tar\.gz")
            
        elif download_name == "cnil":
            pattern_dates = [f"CNIL_{date}" for date in dates] + [f"Freemium_cnil_global_{date}" for date in dates]
            patterns = [re.compile(fr"{pattern}-[0-9]{{6}}\.tar\.gz") for pattern in pattern_dates]
            regular_pattern = re.compile(r"(CNIL|Freemium_cnil_global)_\d{8}-\d{6}\.tar\.gz")
            
        elif download_name == "constit":
            pattern_dates = [f"CONSTIT_{date}" for date in dates] + [f"Freemium_constit_global_{date}" for date in dates]
            patterns = [re.compile(fr"{pattern}-[0-9]{{6}}\.tar\.gz") for pattern in pattern_dates]
            regular_pattern = re.compile(r"(CONSTIT|Freemium_constit_global)_\d{8}-\d{6}\.tar\.gz")
            
        elif download_name == "dole":
            pattern_dates = [f"DOLE_{date}" for date in dates] + [f"Freemium_dole_global_{date}" for date in dates]
            patterns = [re.compile(fr"{pattern}-[0-9]{{6}}\.tar\.gz") for pattern in pattern_dates]
            regular_pattern = re.compile(r"(DOLE|Freemium_dole_global)_\d{8}-\d{6}\.tar\.gz")

        downloader = DilaDownloader(config, f"data/unprocessed/{download_name}", pattern=regular_pattern)
    elif config.get('type') == "directory" or config.get('type') == "sheets":
        from src.download.directory import DirectoryDownloader
        downloader = DirectoryDownloader(config, f"data/unprocessed/{download_name}")
        patterns = []
        
    else :
        raise ValueError(f"Download name '{download_name}' is not supported for downloading.")    
    downloader.download_all(max_download=-1, patterns=patterns)