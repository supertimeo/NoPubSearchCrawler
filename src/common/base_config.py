from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel


class BaseConfig(BaseModel):
    @classmethod
    def load_from_yml(cls: type[Self], path: Path) -> Self:
        with open(path, mode="r") as f:
            return cls.model_validate(yaml.safe_load(f) or {})

    def save_to_yml(self, path: Path) -> None:
        with open(path, mode="w") as f:
            yaml.dump(self.model_dump(), f)