import asyncio
import logging
from typing import Set

from angelovich.core.DataStorage import Entity
from angelovich.core.System import System, TimeSystem

from yap_torrent.components.torrent_ec import TorrentEC
from yap_torrent.env import Env
from yap_torrent.protocol import InfoHash
from yap_torrent.systems import get_info_hash, get_torrent_name, is_torrent_active, is_torrent_complete

logger = logging.getLogger(__name__)

SWEEP_INTERVAL = 5.0


class CompletionSystem(TimeSystem, System):
	"""With `stop_on_complete` on, a torrent never enters the seeding phase: it is stopped.

	Stopping is the same as the user pressing stop — state Inactive, every peer dropped, a
	"stopped" announce — so a finished torrent goes fully quiet: no peers, no incoming
	connections accepted, no tracker or DHT announces.

	The completion event catches a download as it finishes; the sweep catches every other
	way a finished torrent can end up active (restored from a previous session, started by
	hand, re-checked, or its file selection narrowed to what is already on disk).
	"""

	def __init__(self, env: Env):
		System.__init__(self, env)
		TimeSystem.__init__(self, SWEEP_INTERVAL)
		self._stopping: Set[InfoHash] = set()

	async def start(self):
		self.add_listener("action.torrent.complete", self._on_torrent_complete)
		self.add_listener("action.torrent.start", self._on_torrent_start)

	async def _update(self, delta_time: float):
		for torrent_entity in list(self.env.data_storage.get_collection(TorrentEC)):
			await self._stop_if_finished(torrent_entity)

	async def _on_torrent_complete(self, torrent_entity: Entity):
		# one turn of the loop lets the "completed" announce, raised by the same event, take
		# the tracker first, so the tracker hears "completed" before "stopped"
		await asyncio.sleep(0)
		await self._stop_if_finished(torrent_entity)

	async def _on_torrent_start(self, torrent_entity: Entity):
		await asyncio.sleep(0)  # same: let "started" go out before "stopped"
		await self._stop_if_finished(torrent_entity)

	async def _stop_if_finished(self, torrent_entity: Entity):
		if not self.env.config.stop_on_complete:
			return
		if not (is_torrent_active(torrent_entity) and is_torrent_complete(torrent_entity)):
			return

		info_hash = get_info_hash(torrent_entity)
		if info_hash in self._stopping:
			return
		self._stopping.add(info_hash)
		try:
			logger.info("Download finished, stopping instead of seeding: %s", get_torrent_name(torrent_entity))
			await self.env.event_bus.dispatch_async("request.torrent.stop", info_hash)
		finally:
			self._stopping.discard(info_hash)
