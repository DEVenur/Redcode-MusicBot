# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import base64
import json
import os.path
import re
import time
import traceback
from tempfile import gettempdir
from typing import Optional, TYPE_CHECKING, Union
from urllib.parse import quote

import aiofiles
from aiohttp import ClientSession
from rapidfuzz import fuzz

from utils.music.converters import fix_characters, URL_REG
from utils.music.errors import GenericError
from utils.music.models import LavalinkTrack, LavalinkPlaylist
from utils.music.track_encoder import encode_track

if TYPE_CHECKING:
    from utils.client import BotCore

spotify_regex = re.compile("https://open.spotify.com?.+(album|playlist|artist|track)/([a-zA-Z0-9]+)")
spotify_link_regex = re.compile(r"(?i)https?:\/\/spotify\.link\/?(?P<id>[a-zA-Z0-9]+)")
spotify_regex_w_user = re.compile("https://open.spotify.com?.+(album|playlist|artist|track|user)/([a-zA-Z0-9]+)")

spotify_cache_file = os.path.join(gettempdir(), ".spotify_cache.json")


class SpotifyClient:

    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None, playlist_extra_page_limit: int = 0):

        if not client_id:
            raise Exception(
                "CLIENT_ID do spotify não informado."
            )

        if not client_secret:
            raise Exception(
                "CLIENT_SECRET do spotify não informado."
            )

        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.spotify.com/v1"
        self.spotify_cache = {}
        self.disabled = False
        self.playlist_extra_page_limit = playlist_extra_page_limit

        # --- CORREÇÃO 1: Gerenciamento de Sessão aiohttp e Lock ---
        self.session: Optional[ClientSession] = None
        self._token_lock = asyncio.Lock()
        # ---------------------------------------------------------

        try:
            with open(spotify_cache_file) as f:
                self.spotify_cache = json.load(f)
        except FileNotFoundError:
            pass

    # --- ADICIONADO: Método para obter/criar a sessão aiohttp ---
    async def _get_session(self) -> ClientSession:
        if self.session is None or self.session.closed:
            self.session = ClientSession()
        return self.session

    # --- ADICIONADO: Método para fechar a sessão ao desligar o bot ---
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def request(self, path: str, params: dict = None, retries: int = 2):
        
        if self.disabled:
            return

        session = await self._get_session() # <--- CORREÇÃO: Usa a sessão compartilhada
        headers = {'Authorization': f'Bearer {await self.get_valid_access_token()}'}

        for i in range(retries):
            async with session.get(f"{self.base_url}/{path}", headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                
                # <--- CORREÇÃO: Lógica de repetição mais segura que a recursão
                if response.status == 401 and i < retries - 1:
                    print("⚠️ - Spotify: Token inválido/expirado, tentando obter um novo...")
                    await self.get_access_token(force=True)
                    headers['Authorization'] = f'Bearer {await self.get_valid_access_token()}'
                    continue # Tenta novamente no laço

                elif response.status == 404:
                    raise GenericError("**Não houve resultado para o link informado (confira se o link está correto ou se o conteúdo dele está privado ou se foi deletado).**\n\n"
                                       f"{str(response.url).replace('api.', 'open.').replace('/v1/', '/').replace('s/', '/')}")
                
                elif response.status == 429:
                    self.disabled = True
                    print(f"⚠️ - Spotify: Suporte interno (fallback) desativado devido a ratelimit (429).")
                    return
                
                else:
                    response.raise_for_status()

    async def get_track_info(self, track_id: str):
        return await self.request(path=f'tracks/{track_id}')

    async def get_album_info(self, album_id: str):
        return await self.request(path=f'albums/{album_id}')

    async def get_artist_top(self, artist_id: str):
        return await self.request(path=f'artists/{artist_id}/top-tracks')

    async def get_playlist_info(self, playlist_id: str):

        result = await self.request(path=f"playlists/{playlist_id}")

        if result and len(result["tracks"]["items"]) == 100 and self.playlist_extra_page_limit > 0:

            offset = 100 # Começa na faixa 101
            page_count = 0

            while page_count < self.playlist_extra_page_limit:
                try:
                    result_extra = await self.request(path=f"playlists/{playlist_id}/tracks?offset={offset}&limit=100")
                    if not result_extra or not result_extra.get("items"):
                        break
                except:
                    traceback.print_exc()
                    break
                
                result["tracks"]["items"].extend(result_extra["items"])
                
                if result_extra.get("next"):
                    offset += 100
                    page_count += 1
                else:
                    break

        return result

    async def get_user_info(self, user_id: str):
        return await self.request(path=f"users/{user_id}")

    async def get_user_playlists(self, user_id: str):
        return await self.request(path=f"users/{user_id}/playlists")

    async def get_recommendations(self, seed_tracks: Union[list, str], limit=10):
        if isinstance(seed_tracks, str):
            track_ids = seed_tracks
        else:
            track_ids = ",".join(seed_tracks)

        return await self.request(path='recommendations', params={
            'seed_tracks': track_ids, 'limit': limit
        })

    async def track_search(self, query: str):
        return await self.request(path='search', params = {
        'q': quote(query), 'type': 'track', 'limit': 10
        })

    async def get_access_token(self, force=False):

        # <--- CORREÇÃO: Usa asyncio.Lock para evitar race conditions ---
        async with self._token_lock:
            # Verifica novamente dentro do lock para garantir
            if not force and time.time() < self.spotify_cache.get("expires_at", 0):
                return

            try:
                token_url = 'https://accounts.spotify.com/api/token'
                session = await self._get_session()

                headers = {
                    'Authorization': 'Basic ' + base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
                }

                data = {
                    'grant_type': 'client_credentials'
                }

                async with session.post(token_url, headers=headers, data=data) as response:
                    data = await response.json()

                if data.get("error"):
                    print(f"⚠️ - Spotify Fallback: Ocorreu um erro ao obter token: {data['error_description']}")
                    self.disabled = True
                    return

                self.spotify_cache = data
                self.spotify_cache["type"] = "api" # <--- CORREÇÃO: Erro de digitação "tyoe" corrigido
                self.spotify_cache["expires_at"] = time.time() + data["expires_in"]

                print("🎶 - Access token do spotify (fallback) obtido com sucesso via API Oficial.")

                async with aiofiles.open(spotify_cache_file, "w") as f:
                    await f.write(json.dumps(self.spotify_cache))

            except Exception:
                self.disabled = True
                traceback.print_exc()
                print(f"⚠️ - Spotify Fallback: Falha crítica ao obter token, desativando...")

    async def get_valid_access_token(self):
        if not self.spotify_cache or time.time() >= self.spotify_cache.get("expires_at", 0):
            await self.get_access_token(force=True)
        
        return self.spotify_cache.get("access_token")

    async def get_tracks(self, bot: BotCore, requester: int, query: str, search: bool = True, check_title: float = None):

        # ############################################################### #
        # OBJETIVO PRINCIPAL: USAR LAVASRC PRIMEIRO                       #
        # ############################################################### #
        # Verifica se algum node conectado tem o Lavasrc ativo para Spotify.
        if any("spotify" in n.info.get("sourceManagers", []) for n in bot.music.nodes.values()):
            # Se tiver, retorna None. O código do comando 'play' vai entender
            # que deve passar a query diretamente para a busca do Lavalink.
            return None
        # ############################################################### #

        # O código abaixo só será executado se o Lavasrc não estiver disponível (fallback).
        
        print("LOG: Usando o fallback spotify.py, pois Lavasrc não foi detectado.")

        if spotify_link_regex.match(query):
            async with bot.session.get(query, allow_redirects=False) as r:
                if 'location' not in r.headers:
                    raise GenericError("**Falha ao obter resultado para o link informado...**")
                query = str(r.headers["location"])

        if not (matches := spotify_regex.match(query)) and not self.disabled:

            if URL_REG.match(query) or not search:
                return

            r = await self.track_search(query=query)

            if not r: # Se a busca falhar (ex: rate limit)
                return

            tracks = []

            try:
                tracks_result = r['tracks']['items']
            except KeyError:
                pass
            else:
                for result in tracks_result:

                    trackinfo = {
                        'title': result["name"],
                        'author': result["artists"][0]["name"] or "Unknown Artist",
                        'length': result["duration_ms"],
                        'identifier': result["id"],
                        'isStream': False,
                        'uri': result["external_urls"]["spotify"],
                        'sourceName': 'spotify',
                        'position': 0,
                        'artworkUrl': result["album"]["images"][0]["url"] if result.get("album", {}).get("images") else "",
                    }

                    try:
                        trackinfo["isrc"] = result["external_ids"]["isrc"]
                    except KeyError:
                        pass

                    t = LavalinkTrack(id_=encode_track(trackinfo)[1], info=trackinfo, requester=requester)

                    t.info["extra"]["authors"] = [fix_characters(i['name']) for i in result['artists'] if f"feat. {i['name'].lower()}"
                                                  not in result['name'].lower()]

                    if check_title and fuzz.token_sort_ratio(query.lower(), f"{t.authors_string} - {t.single_title}".lower()) < check_title:
                        continue
                    
                    t.info["extra"]["authors_md"] = ", ".join(f"[`{a['name']}`]({a['external_urls']['spotify']})" for a in result["artists"])

                    try:
                        if result["album"]["name"] != result["name"]:
                            t.info["extra"]["album"] = {
                                "name": result["album"]["name"],
                                "url": result["album"]["external_urls"]["spotify"]
                            }
                    except (AttributeError, KeyError):
                        pass

                    tracks.append(t)

                return tracks

            return
        
        if self.disabled:
            raise GenericError("**O suporte a links do spotify (fallback) está temporariamente desativado.**")

        url_type, url_id = matches.groups()

        if url_type == "track":

            result = await self.get_track_info(url_id)

            trackinfo = {
                'title': result["name"],
                'author': result["artists"][0]["name"] or "Unknown Artist",
                'length': result["duration_ms"],
                'identifier': result["id"],
                'isStream': False,
                'uri': result["external_urls"]["spotify"],
                'sourceName': 'spotify',
                'position': 0,
                'artworkUrl': result["album"]["images"][0]["url"] if result.get("album", {}).get("images") else "",
            }

            try:
                trackinfo["isrc"] = result["external_ids"]["isrc"]
            except KeyError:
                pass

            t = LavalinkTrack(id_=encode_track(trackinfo)[1], info=trackinfo, requester=requester)

            t.info["extra"]["authors"] = [fix_characters(i['name']) for i in result['artists'] if f"feat. {i['name'].lower()}"
                                          not in result['name'].lower()]

            t.info["extra"]["authors_md"] = ", ".join(f"[`{a['name']}`]({a['external_urls']['spotify']})" for a in result["artists"])

            try:
                if result["album"]["name"] != result["name"] or result["album"]["total_tracks"] > 1:
                    t.info["extra"]["album"] = {
                        "name": result["album"]["name"],
                        "url": result["album"]["external_urls"]["spotify"]
                    }
            except (AttributeError, KeyError):
                pass

            return [t]

        data = {
            'loadType': 'PLAYLIST_LOADED',
            'playlistInfo': {'name': ''},
            'sourceName': "spotify",
            'is_album': False,
            "thumb": ""
        }

        tracks_data = []

        if url_type == "album":

            cache_key = f"partial:spotify:{url_type}:{url_id}"

            if not (result := bot.pool.playlist_cache.get(cache_key)):
                result = await self.get_album_info(url_id)
                bot.pool.playlist_cache[cache_key] = result

            if not result or not result.get("tracks"):
                raise GenericError("**Não houve resultados para o link do álbum informado...**")

            data["playlistInfo"]["name"] = result["name"]
            data["playlistInfo"]["is_album"] = True

            for t in result["tracks"]["items"]:
                if t:
                    t["album"] = result

            tracks_data = result["tracks"]["items"]

        elif url_type == "artist":

            cache_key = f"partial:spotify:{url_type}:{url_id}"

            if not (result := bot.pool.playlist_cache.get(cache_key)):
                result = await self.get_artist_top(url_id)
                bot.pool.playlist_cache[cache_key] = result
            
            if not result or not result.get("tracks"):
                raise GenericError("**Não foi possível obter as músicas do artista.**")

            try:
                data["playlistInfo"]["name"] = "As mais tocadas de: " + \
                                               [a["name"] for a in result["tracks"][0]["artists"] if a["id"] == url_id][0]
            except IndexError:
                data["playlistInfo"]["name"] = "As mais tocadas de: " + result["tracks"][0]["artists"][0]["name"]
            tracks_data = result["tracks"]

        elif url_type == "playlist":

            cache_key = f"partial:spotify:{url_type}:{url_id}"

            if not (result := bot.pool.playlist_cache.get(cache_key)):
                result = await self.get_playlist_info(url_id)
                bot.pool.playlist_cache[cache_key] = result

            if not result or not result.get("tracks", {}).get("items"):
                raise GenericError("**Não houve resultados para o link da playlist informada...**")
            
            data["playlistInfo"]["name"] = result["name"]
            if result.get("images"):
                data["playlistInfo"]["thumb"] = result["images"][0]["url"]

            tracks_data = [t["track"] for t in result["tracks"]["items"] if t.get("track")]

        else:
            raise GenericError(f"**Link do spotify não reconhecido/suportado:**\n{query}")

        if not tracks_data:
            raise GenericError("**Não houve resultados para o link do spotify informado...**")

        playlist = LavalinkPlaylist(data, url=query)

        playlist_info = playlist if url_type != "album" else None

        for t in tracks_data:

            if not t:
                continue

            try:
                thumb = t["album"]["images"][0]["url"]
            except (IndexError, KeyError):
                thumb = ""

            trackinfo = {
                'title': t["name"],
                'author': t["artists"][0]["name"] or "Unknown Artist",
                'length': t["duration_ms"],
                'identifier': t["id"],
                'isStream': False,
                'uri': t["external_urls"].get("spotify", ""),
                'sourceName': 'spotify',
                'position': 0,
                'artworkUrl': thumb,
            }

            try:
                trackinfo["isrc"] = t["external_ids"]["isrc"]
            except KeyError:
                pass

            track = LavalinkTrack(id_=encode_track(trackinfo)[1], info=trackinfo, requester=requester, playlist=playlist_info)

            try:
                if t["album"]["name"] != t["name"] or t["album"]["total_tracks"] > 1:
                    track.info["extra"]["album"] = {
                        "name": t["album"]["name"],
                        "url": t["album"]["external_urls"]["spotify"]
                    }
            except (AttributeError, KeyError):
                pass

            if t.get("artists"):
                track.info["extra"]["authors"] = [fix_characters(i['name']) for i in t['artists'] if f"feat. {i['name'].lower()}" not in t['name'].lower()]
                track.info["extra"]["authors_md"] = ", ".join(f"[`{fix_characters(a['name'])}`](<{a['external_urls'].get('spotify', '')}>)" for a in t['artists'])
            else:
                track.info["extra"]["authors"] = ["Unknown Artist"]
                track.info["extra"]["authors_md"] = "`Unknown Artist`"

            playlist.tracks.append(track)

        return playlist
