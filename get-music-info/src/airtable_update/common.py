from typing import Any, TypedDict

from airtable import Airtable  # type: ignore

from more_itertools import nth


class AirtableRecordReadModel(TypedDict):
    id: str
    fields: dict[str, Any]
    createdTime: str


async def get_or_create_airtable_record(
    airtable: Airtable,
    search_field: str,
    search_value: Any,
    new_fields: dict[str, Any],
) -> str:
    results: list[AirtableRecordReadModel] = \
        airtable.search(  # type: ignore
            search_field,
            search_value,
    )

    if len(results) > 1:
        raise RuntimeError((
            f"{len(results)} records in {airtable.table_name} "  # type: ignore
            f"found for {search_field} equal to '{search_value}'"
        ))

    result = nth(results, 0)

    if result is not None:
        return result["id"]

    record: AirtableRecordReadModel = airtable.insert(  # type: ignore
        new_fields,
    )

    return record["id"]
