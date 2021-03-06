import logging

from aiohttp import ClientSession

from fastapi import FastAPI

from .airtable_update import update_all_tracks
from .spotify import SpotifyClient

logging.basicConfig(level=logging.DEBUG)

client_session = ClientSession()

spotify_client = SpotifyClient(client_session)

app = FastAPI()


@app.on_event("shutdown")  # type: ignore
async def shutdown() -> None:
    await client_session.close()


@app.post("/update_airtable")
async def update_airtable() -> str:
    updated_tracks = await update_all_tracks(spotify_client)
    return f"Updated {updated_tracks} track(s)"
