from airtable import Airtable  # type: ignore

from .config import api_key, base_key

artist_airtable = Airtable(
    base_key=base_key,
    table_name="Artist",
    api_key=api_key,
)

label_airtable = Airtable(
    base_key=base_key,
    table_name="Label",
    api_key=api_key,
)

release_airtable = Airtable(
    base_key=base_key,
    table_name="Release",
    api_key=api_key,
)

track_airtable = Airtable(
    base_key=base_key,
    table_name="Track",
    api_key=api_key,
)
