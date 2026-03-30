from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_path: Path = Path("proteinvault.duckdb")
    model_cache_dir: Path = Path.home() / ".cache" / "proteinvault" / "models"
    data_cache_dir: Path = Path.home() / ".cache" / "proteinvault" / "data"

    model_config = {"env_prefix": "PROTEINVAULT_"}


settings = Settings()
