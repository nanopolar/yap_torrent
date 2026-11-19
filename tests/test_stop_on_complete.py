"""stop_on_complete: a finished torrent is stopped instead of seeded."""
import asyncio
from pathlib import Path

from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentEC, TorrentState, TorrentStatsEC
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Bitfield, Metainfo, TrackerAnnounceResponse
from yap_torrent.systems import create_torrent_entity
from yap_torrent.systems import announce_system as announce_module
from yap_torrent.systems.announce_system import AnnounceSystem
from yap_torrent.systems.completion_system import CompletionSystem
from yap_torrent.systems.dht_system import DHTSystem
from yap_torrent.systems.torrents_system import TorrentSystem

PIECES = 4


def _env(tmp_path: Path = None) -> Env:
	config = Config(path="__none__.json")
	if tmp_path is not None:
		config.data_folder = tmp_path
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", config)


def _meta(name: bytes = b"t") -> Metainfo:
	info = {"name": name, "piece length": 16384, "pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES}
	return Metainfo(decode(encode({"info": info})))


def _torrent(env: Env, have=range(PIECES), name: bytes = b"t"):
	meta = _meta(name)
	torrent = create_torrent_entity(env, meta.make_info_hash(), Path("."), {}, meta.info)
	for index in have:
		torrent.get_component(TorrentEC).bitfield.set_index(index)
	wanted = Bitfield()
	for index in range(PIECES):
		wanted.set_index(index)
	torrent.add_component(TorrentDownloadProgressEC(wanted))
	return torrent


def _state(torrent) -> TorrentState:
	return torrent.get_component(TorrentStatsEC).state


async def _systems(env: Env, *extra):
	systems = [TorrentSystem(env), CompletionSystem(env), *extra]
	for system in systems:
		await system.start()
	return systems


def test_it_is_on_by_default():
	assert Config(path="__none__.json").stop_on_complete is True


def test_a_finished_torrent_is_stopped_when_it_completes():
	async def run():
		env = _env()
		torrent = _torrent(env)
		stopped = []

		async def on_stop(entity):
			stopped.append(entity)

		env.event_bus.add_listener("action.torrent.stop", on_stop)
		await _systems(env)

		await asyncio.gather(*env.event_bus.dispatch("action.torrent.complete", torrent))

		assert _state(torrent) == TorrentState.Inactive
		assert stopped == [torrent]  # peers are dropped and "stopped" announced off this event

	asyncio.run(run())


def test_the_sweep_stops_a_finished_torrent_that_is_already_running():
	# e.g. restored from a previous session, where no completion event fires
	async def run():
		env = _env()
		finished = _torrent(env, name=b"done")
		unfinished = _torrent(env, have=range(PIECES - 1), name=b"partial")
		_, completion = await _systems(env)

		await completion._update(0)

		assert _state(finished) == TorrentState.Inactive
		assert _state(unfinished) == TorrentState.Active

	asyncio.run(run())


def test_starting_a_finished_torrent_stops_it_again():
	async def run():
		env = _env()
		torrent = _torrent(env)
		torrent.get_component(TorrentStatsEC).state = TorrentState.Inactive
		await _systems(env)

		await env.event_bus.dispatch_async("request.torrent.start", torrent.get_component(TorrentEC).info_hash)
		await asyncio.sleep(0.01)  # the start handler yields once before stopping

		assert _state(torrent) == TorrentState.Inactive

	asyncio.run(run())


def test_nothing_is_stopped_when_the_option_is_off():
	async def run():
		env = _env()
		env.config.stop_on_complete = False
		torrent = _torrent(env)
		_, completion = await _systems(env)

		await asyncio.gather(*env.event_bus.dispatch("action.torrent.complete", torrent))
		await completion._update(0)

		assert _state(torrent) == TorrentState.Active

	asyncio.run(run())


def test_the_tracker_hears_completed_before_stopped(monkeypatch):
	async def run():
		env = _env()
		torrent = _torrent(env)
		torrent.add_component(TorrentTrackerEC([["http://tracker.test/announce"]]))
		torrent.add_component(TorrentTrackerDataEC())
		events = []

		async def slow_tracker(announce, info_hash, **kwargs):
			event = kwargs.get("event")
			if event == "completed":
				await asyncio.sleep(0.05)  # a slow tracker must not let "stopped" overtake it
			events.append(event)
			return TrackerAnnounceResponse({"interval": 1800, "peers": b""}, 1)

		monkeypatch.setattr(announce_module, "make_announce", slow_tracker)
		await _systems(env, AnnounceSystem(env))
		await asyncio.sleep(0.01)
		events.clear()  # drop the "started" from AnnounceSystem.start

		await asyncio.gather(*env.event_bus.dispatch("action.torrent.complete", torrent))
		await asyncio.sleep(0.2)

		assert events == ["completed", "stopped"]
		assert _state(torrent) == TorrentState.Inactive

	asyncio.run(run())


def test_dht_does_not_look_up_a_stopped_torrent(tmp_path):
	async def run():
		env = _env(tmp_path)
		torrent = _torrent(env)
		info_hash = torrent.get_component(TorrentEC).info_hash
		dht = DHTSystem(env)
		dht.pending_nodes.clear()
		lookups = []

		async def fake_find_peers(ih):
			lookups.append(ih)

		dht._find_peers = fake_find_peers

		torrent.get_component(TorrentStatsEC).state = TorrentState.Inactive
		dht.pending_torrents.append(info_hash)
		await dht._update(0)
		await asyncio.sleep(0)
		assert lookups == [] and dht.pending_torrents == []

		torrent.get_component(TorrentStatsEC).state = TorrentState.Active
		dht.pending_torrents.append(info_hash)
		await dht._update(0)
		await asyncio.sleep(0)
		assert lookups == [info_hash]

	asyncio.run(run())
