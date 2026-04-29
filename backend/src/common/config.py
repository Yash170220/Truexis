"""Configuration management using Pydantic Settings"""
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Database configuration"""
    url: str = Field(..., alias="DATABASE_URL")
    echo: bool = Field(default=False, alias="DB_ECHO")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class RedisConfig(BaseSettings):
    """Redis cache configuration"""
    url: str = Field(..., alias="REDIS_URL")
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class ClaudeConfig(BaseSettings):
    """Anthropic Claude API configuration"""
    api_key: str = Field(..., alias="ANTHROPIC_API_KEY")
    model: str = Field(default="claude-haiku-4-5", alias="ANTHROPIC_MODEL")
    temperature: float = Field(default=0.7, alias="ANTHROPIC_TEMPERATURE")
    max_tokens: int = Field(default=2048, alias="ANTHROPIC_MAX_TOKENS")

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        if not v or v == "sk-ant-...":
            raise ValueError("ANTHROPIC_API_KEY must be set")
        return v

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class AppConfig(BaseSettings):
    """Application configuration"""
    name: str = Field(default="ai-esg-reporting", alias="APP_NAME")
    debug: bool = Field(default=False, alias="DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    max_file_size_mb: int = Field(default=50, alias="MAX_FILE_SIZE_MB")
    allowed_extensions: list[str] = Field(
        default=[".xlsx", ".csv", ".pdf"], 
        alias="ALLOWED_EXTENSIONS"
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of {valid_levels}")
        return v.upper()

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class MatchingConfig(BaseSettings):
    """Matching configuration"""
    confidence_threshold: float = Field(default=0.80, alias="MATCHING_CONFIDENCE_THRESHOLD")
    review_threshold: float = Field(default=0.85, alias="MATCHING_REVIEW_THRESHOLD")
    llm_threshold: float = Field(default=0.70, alias="MATCHING_LLM_THRESHOLD")

    @field_validator("confidence_threshold", "review_threshold", "llm_threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("Threshold must be between 0.0 and 1.0")
        return v

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class Settings(BaseSettings):
    """Global settings"""
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    claude: ClaudeConfig = Field(default_factory=ClaudeConfig)
    app: AppConfig = Field(default_factory=AppConfig)
    matching: MatchingConfig = Field(default_factory=MatchingConfig)

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Global settings instance
settings = Settings()
