import json
import re
import time
from base64 import b64encode
from typing import NamedTuple, Optional, Sequence, TypedDict
from urllib.parse import ParseResult, urlparse, urlunparse

from aiohttp.client import ClientResponse, ClientSession

import typedload

from .os_utils import getenvx

client_id = getenvx("SPOTIFY_CLIENT_ID")
client_secret = getenvx("SPOTIFY_CLIENT_SECRET")

track_path_regex = re.compile(
    """
    /track/
    ([a-zA-Z0-9]+)
    """,
    re.VERBOSE,
)


def parse_track_id(track_url: str) -> Optional[str]:
    parsed_url = urlparse(track_url)

    if parsed_url.netloc != "open.spotify.com":
        return None

    track_path = track_path_regex.match(parsed_url.path)

    if track_path is None:
        return None

    return track_path.group(1)


def get_track_url(track_id: str) -> str:
    return urlunparse(
        ParseResult(
            scheme="https",
            netloc="open.spotify.com",
            path=f"/track/{track_id}",
            params="",
            query="",
            fragment="",
        ),
    )


class SpotifyAccessTokenResponseBody(TypedDict):
    access_token: str
    token_type: str
    expires_in: int


class SpotifyAccessToken(NamedTuple):
    access_token: str
    expires_at_s: float


class SpotifyAudioFeatures(TypedDict):
    """
    See the docs: https://developer.spotify.com/documentation/web-api/reference/#object-audiofeaturesobject  # noqa
    """
    acousticness: float
    analysis_url: str
    danceability: float
    duration_ms: int
    energy: float
    id: str
    instrumentalness: float
    key: int
    liveness: float
    loudness: float
    mode: int
    speechiness: float
    tempo: float
    time_signature: int
    track_href: str
    type: str
    uri: str
    valence: float


class SpotifyAudioFeaturesForTracks(TypedDict):
    audio_features: list[SpotifyAudioFeatures]


class SpotifyTrackExternalUrls(TypedDict):
    spotify: str


class SpotifySearchTrack(TypedDict):
    id: str
    name: str
    external_urls: SpotifyTrackExternalUrls


class SpotifySearchTracks(TypedDict):
    items: list[SpotifySearchTrack]  # type: ignore


class SpotifySearchResponse(TypedDict):
    tracks: SpotifySearchTracks


class SpotifyClient:
    ACCESS_TOKEN_ENDPOINT = "https://accounts.spotify.com/api/token"
    AUDIO_FEATURES_ENDPOINT = "https://api.spotify.com/v1/audio-features"
    SEARCH_ENDPOINT = "https://api.spotify.com/v1/search"

    def __init__(self, session: ClientSession) -> None:
        self.session = session
        self.last_access_token: Optional[SpotifyAccessToken] = None

    async def get_access_token(self) -> str:
        if (
            self.last_access_token is not None and
            self.last_access_token.expires_at_s > (time.time() + 5)
        ):
            return self.last_access_token.access_token

        authorization = \
            b64encode(f"{client_id}:{client_secret}".encode()).decode()

        async with self.session.post(
            self.ACCESS_TOKEN_ENDPOINT,
            headers={"Authorization": f"Basic {authorization}"},
            data={"grant_type": "client_credentials"},
        ) as response:
            if response.status != 200:
                raise RuntimeError("Unable to get Spotify client credentials")

            response_body_raw = await response.text()

            response_body = typedload.load(  # type: ignore
                json.loads(response_body_raw),
                SpotifyAccessTokenResponseBody,
            )

            self.last_access_token = SpotifyAccessToken(
                access_token=response_body["access_token"],
                expires_at_s=time.time() + response_body["expires_in"],
            )

            return response_body["access_token"]

    async def get(self, url: str) -> ClientResponse:
        access_token = await self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        return await self.session.get(url, headers=headers)

    async def search_for_tracks(self, q: str) -> list[SpotifySearchTrack]:
        response = await self.get(
            f"{self.SEARCH_ENDPOINT}?q={q}&type=track&limit=10",
        )

        response_json = await response.json()

        response.close()

        response_parsed = typedload.load(  # type: ignore
            response_json,
            SpotifySearchResponse,
        )

        return response_parsed["tracks"]["items"]

    async def get_audio_features_for_tracks(
        self,
        track_ids: Sequence[str],
    ) -> list[SpotifyAudioFeatures]:
        if len(track_ids) == 0:
            raise ValueError("Need to provide at least one track ID")

        track_ids_csv = ",".join(track_ids)

        response = await self.get(
            f"{self.AUDIO_FEATURES_ENDPOINT}?ids={track_ids_csv}",
        )

        response_json = await response.json()

        response_parsed = typedload.load(  # type: ignore
            response_json,
            SpotifyAudioFeaturesForTracks,
        )

        response.close()

        return response_parsed["audio_features"]
