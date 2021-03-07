import logging

from aiohttp import ClientSession

from fastapi import FastAPI, Response, status

import requests

from .airtable_update.releases import update_all_releases
from .airtable_update.tracks import update_all_tracks
from .spotify import SpotifyClient

logging.basicConfig(level=logging.DEBUG)

client_session = ClientSession()

spotify_client = SpotifyClient(client_session)

app = FastAPI()


@app.on_event("shutdown")  # type: ignore
async def shutdown_handler() -> None:
    await client_session.close()


@app.post("/update_all_tracks", response_model=str)
async def update_all_tracks_handler() -> str:
    updated_tracks = await update_all_tracks(spotify_client)
    return f"Updated {updated_tracks} track(s)"


@app.post("/update_all_releases", response_model=str)
async def update_all_releases_handler(response: Response) -> str:
    try:
        updated_tracks = await update_all_releases()
    except requests.exceptions.HTTPError as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return f"API error: {e}"

    return f"Updated {updated_tracks} release(s)"
