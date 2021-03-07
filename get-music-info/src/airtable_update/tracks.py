import asyncio
from typing import Optional, Sequence, TypedDict

from more_itertools import nth

from .airtables import track_airtable
from ..spotify import SpotifyClient, get_track_url, parse_track_id

KEY_MAPPING = {
    0: "C",
    1: "C#",
    2: "D",
    3: "D#",
    4: "E",
    5: "F",
    6: "F#",
    7: "G",
    8: "G#",
    9: "A",
    10: "A#",
    11: "B",
}

MODE_MAPPING = {
    0: "minor",
    1: "major",
}


class AirtableTrackFields(TypedDict, total=False):
    Album: str
    Artist: str
    Spotify: str
    Title: str


class AirtableTrack(TypedDict):
    id: str
    fields: AirtableTrackFields
    createdTime: str


async def get_track_spotify_id(
    spotify_client: SpotifyClient,
    airtable_track: AirtableTrack,
) -> Optional[str]:
    airtable_fields = airtable_track["fields"]

    try:
        track_id_from_url = parse_track_id(airtable_fields["Spotify"])
    except Exception:
        track_description = " - ".join([
            airtable_fields["Artist"],
            airtable_fields["Title"],
        ])

        tracks = await spotify_client.search_for_tracks(track_description)

        maybe_track = nth(tracks, 0)

        track_id_from_url = \
            maybe_track["id"] if maybe_track is not None else None

    return track_id_from_url


async def update_airtable_tracks(
    spotify_client: SpotifyClient,
    tracks: Sequence[AirtableTrack],
) -> int:
    spotify_ids = await asyncio.gather(
        *map(
            lambda track: get_track_spotify_id(spotify_client, track),
            tracks,
        ),
    )

    tracks_by_spotify_id = {
        spotify_id: track
        for spotify_id, track in zip(spotify_ids, tracks)
        if spotify_id is not None
    }

    if len(tracks_by_spotify_id) == 0:
        return 0

    audio_features_for_all_tracks = \
        await spotify_client.get_audio_features_for_tracks(
            list(tracks_by_spotify_id.keys()),
        )

    audio_features_by_spotify_id = {
        audio_features["id"]: audio_features
        for audio_features in audio_features_for_all_tracks
    }

    records_to_update = [{
        "id": tracks_by_spotify_id[spotify_id]["id"],
        "fields": {
            "Duration": audio_features["duration_ms"] / 1000,
            "BPM": audio_features["tempo"],
            "Signature": audio_features["time_signature"],
            "Key": KEY_MAPPING[audio_features["key"]],
            "Mode": MODE_MAPPING[audio_features["mode"]],
            "Danceability": audio_features["danceability"],
            "Energy": audio_features["energy"],
            "Loudness": audio_features["loudness"],
            "Instrumentalness": audio_features["instrumentalness"],
            "Valence": audio_features["valence"],
            "Spotify": get_track_url(spotify_id),
            "Updated": True,
        },
    } for spotify_id, audio_features in audio_features_by_spotify_id.items()]

    track_airtable.batch_update(records_to_update)  # type: ignore

    return len(records_to_update)


async def update_all_tracks(spotify_client: SpotifyClient) -> int:
    updated_tracks = 0

    for page in track_airtable.get_iter(   # type: ignore
        fields=AirtableTrackFields.__annotations__.keys(),
        formula="""
            AND(
                OR(
                    LEN(Spotify) > 0,
                    AND(
                        LEN(Title) > 0,
                        LEN(Artist) > 0
                    )
                ),
                NOT(Updated)
            )
        """,
    ):
        airtable_tracks = [AirtableTrack(**track) for track in page]

        updated_tracks += await update_airtable_tracks(
            spotify_client,
            airtable_tracks,
        )

    return updated_tracks
