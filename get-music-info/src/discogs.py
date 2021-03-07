import re
from typing import Optional, TypedDict
from urllib.parse import urlparse

import discogs_client  # type: ignore
from discogs_client.models import ListField  # type: ignore


class DiscogsLabel(TypedDict):
    url: str
    name: str


class DiscogsArtist(TypedDict):
    url: str
    name: str


class DiscogsTrack(TypedDict):
    position: str
    title: str
    artists: list[DiscogsArtist]
    credits: list[DiscogsArtist]


class DiscogsRelease(TypedDict):
    url: str
    title: str
    labels: list[DiscogsLabel]
    artists: list[DiscogsArtist]
    credits: list[DiscogsArtist]
    tracklist: list[DiscogsTrack]
    year: int
    genres: list[str]
    styles: list[str]


release_path_regex = re.compile(
    """
    .*
    /release/
    ([a-zA-Z0-9]+)
    """,
    re.VERBOSE,
)

d = discogs_client.Client("kkom/dj")


def parse_release_id(url: str) -> Optional[str]:
    parsed_url = urlparse(url)

    if parsed_url.netloc not in ("discogs.com", "www.discogs.com"):
        return None

    release_path = release_path_regex.match(parsed_url.path)

    if release_path is None:
        return None

    return release_path.group(1)


def get_release_info(discogs_id: str) -> Optional[DiscogsRelease]:
    release = d.release(discogs_id)  # type: ignore

    labels: list[DiscogsLabel] = list(map(  # type: ignore
        lambda l: DiscogsLabel(url=l.url, name=l.name),  # type: ignore
        release.labels,
    ))

    def get_artists(list_field: ListField) -> list[DiscogsArtist]:
        return list(
            map(
                lambda a: DiscogsArtist(  # type: ignore
                    url=a.url,  # type: ignore
                    name=a.name,  # type: ignore
                ),
                list_field,
            ),
        )

    artists = get_artists(release.artists)
    credits = get_artists(release.credits)

    tracklist: list[DiscogsTrack] = list(map(
        lambda t: DiscogsTrack(  # type: ignore
            position=t.position,  # type: ignore
            title=t.title,  # type: ignore
            artists=get_artists(t.artists),  # type: ignore
            credits=get_artists(t.credits),  # type: ignore
        ),
        release.tracklist,
    ))

    return DiscogsRelease(
        url=release.url,  # type: ignore
        title=release.title,  # type: ignore
        labels=labels,
        artists=artists,
        credits=credits,
        tracklist=tracklist,
        year=release.year,  # type: ignore
        genres=release.genres,  # type: ignore
        styles=release.styles,  # type: ignore
    )
