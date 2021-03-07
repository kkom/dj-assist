import asyncio
import logging
from typing import Sequence, TypedDict

import typedload

from .airtables import (
    artist_airtable,
    label_airtable,
    release_airtable,
    track_airtable,
)
from .common import get_or_create_airtable_record
from ..discogs import (
    DiscogsArtist,
    DiscogsLabel,
    DiscogsRelease,
    DiscogsTrack,
    get_release_info,
    parse_release_id,
)


class AirtableArtistWriteFields(TypedDict):
    Name: str
    Discogs: str


class AirtableLabelWriteFields(TypedDict):
    Name: str
    Discogs: str


class AirtableReleaseReadFields(TypedDict):
    Discogs: str


class AirtableReleaseReadModel(TypedDict):
    id: str
    fields: AirtableReleaseReadFields
    createdTime: str


class AirtableReleaseWriteFields(TypedDict):
    Title: str
    Artists: Sequence[str]
    Credits: Sequence[str]
    Labels: Sequence[str]
    Discogs: str
    Updated: bool
    Year: int
    Genres: list[str]
    Styles: list[str]


class AirtableTrackWriteFields(TypedDict):
    Release: list[str]
    Position: str
    Title: str


async def get_or_create_airtable_artist(
    discogs_artist: DiscogsArtist,
) -> str:
    return await get_or_create_airtable_record(
        artist_airtable,
        "Discogs",
        discogs_artist["url"],
        dict(
            AirtableArtistWriteFields(
                Name=discogs_artist["name"],
                Discogs=discogs_artist["url"],
            ),
        ),
    )


async def get_or_create_airtable_label(discogs_label: DiscogsLabel) -> str:
    return await get_or_create_airtable_record(
        label_airtable,
        "Discogs",
        discogs_label["url"],
        dict(
            AirtableLabelWriteFields(
                Name=discogs_label["name"],
                Discogs=discogs_label["url"],
            ),
        ),
    )


async def update_airtable_release(
    airtable_release_id: str,
    discogs_release: DiscogsRelease,
) -> None:
    (
        artists_airtable_ids,
        credits_airtable_ids,
        labels_airtable_ids,
    ) = await asyncio.gather(
        asyncio.gather(
            *map(
                get_or_create_airtable_artist,
                discogs_release["artists"],
            ),
        ),
        asyncio.gather(
            *map(
                get_or_create_airtable_artist,
                discogs_release["credits"],
            ),
        ),
        asyncio.gather(
            *map(
                get_or_create_airtable_label,
                discogs_release["labels"],
            ),
        ),
    )

    release_airtable.update(  # type: ignore
        airtable_release_id,
        AirtableReleaseWriteFields(
            Title=discogs_release["title"],
            Artists=list(set(artists_airtable_ids)),
            Credits=list(set(credits_airtable_ids)),
            Labels=list(set(labels_airtable_ids)),
            Year=discogs_release["year"],
            Genres=discogs_release["genres"],
            Styles=discogs_release["styles"],
            Discogs=discogs_release["url"],
            Updated=True,
        ),
    )


async def create_airtable_tracks(
    airtable_release_id: str,
    discogs_tracks: Sequence[DiscogsTrack],
) -> None:
    track_airtable.batch_insert(  # type: ignore
        list(map(
            lambda discogs_track: AirtableTrackWriteFields(
                Release=[airtable_release_id],
                Position=discogs_track["position"],
                Title=discogs_track["title"],
            ),
            discogs_tracks,
        )),
    )


async def update_release(airtable_release: AirtableReleaseReadModel) -> bool:
    discogs_id = parse_release_id(airtable_release["fields"]["Discogs"])

    if discogs_id is None:
        return False

    discogs_release = get_release_info(discogs_id)

    if discogs_release is None:
        return False

    logging.debug(f"{discogs_release=}")

    # We're creating the Airtable release first, as it may fail, e.g. due to
    # creating a new genre/style, which is not allowed through the API:
    # https://community.airtable.com/t/how-can-i-prevent-the-invalid-multiple-choice-options-error/7392/6 # noqa
    await update_airtable_release(
        airtable_release["id"],
        discogs_release,
    )

    # Now, as the release is properly created, we can create the tracks, as
    # this is much less likely to fail
    await create_airtable_tracks(
        airtable_release["id"],
        discogs_release["tracklist"],
    )

    return True


async def update_all_releases() -> int:
    updated_releases_total = 0

    for page in release_airtable.get_iter(   # type: ignore
        fields=AirtableReleaseReadFields.__annotations__.keys(),
        formula="""
            AND(
                LEN(Discogs) > 0,
                NOT(Updated)
            )
        """,
    ):
        airtable_releases = [
            typedload.load(release, AirtableReleaseReadModel)  # type: ignore
            for release in page
        ]

        updated_releases = await asyncio.gather(
            *map(update_release, airtable_releases),
        )

        updated_releases_total += sum(updated_releases)

    return updated_releases_total
