from enum import Enum
from functools import cached_property

from pydantic import computed_field
from pydantic_settings import BaseSettings


class Environment(str, Enum):
    dev = "DEV"
    prod = "PROD"
    test = "TEST"


class Cloud(str, Enum):
    gcp = "gcp"


class EnvironmentSettings(BaseSettings):
    model_config = {"case_sensitive": True}
    DEBUG: bool = False
    ENV: Environment = Environment.dev
    CLOUD: Cloud = Cloud.gcp

    @computed_field(return_type=str)
    @cached_property
    def cloud_prefix(self):
        if self.CLOUD == Cloud.gcp:
            return "gs://"
        else:
            raise NotImplementedError(f"Cloud {self.CLOUD} not implemented")


settings = EnvironmentSettings()  # type:ignore
