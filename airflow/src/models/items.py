from typing import Optional

from pydantic import BaseModel


class Item(BaseModel):
    id: int
    locale_default: str
    locale_name: dict[str, str]
    grade: int
    category_primary: Optional[str]  # noqa: UP007
    category_secondary: Optional[str]  # noqa: UP007
