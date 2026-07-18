from pathlib import Path
from typing import Final

root_folder_path: Final[Path] = Path(__file__).resolve().parent.parent.parent
log_folder_path: Final[Path] = root_folder_path / "logs"
cache_folder_path: Final[Path] = root_folder_path / "caches"
backup_folder_path: Final[Path] = root_folder_path / "backups"
assets_folder_path: Final[Path] = root_folder_path / "assets"
config_files_folder_path: Final[Path] = root_folder_path / "config_files"
crawler_config_file_path: Final[Path] = config_files_folder_path / "crawler_config.yaml"