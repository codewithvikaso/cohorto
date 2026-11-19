from pydantic_settings import BaseSettings, SettingsConfigDict
class Settings(BaseSettings):
    APP_NAME: str = "Cohort Insights API"
    ENVIRONMENT: str = "development"

    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    MONGODB_URL: str = "mongodb://localhost:27017"
    MONGODB_DATABASE: str = "cohort_insights"

    REDIS_URL: str = "redis://localhost:6379/0"

    MAX_ACTIVE_JOBS: int = 3
    CACHE_TTL: int = 3600

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
