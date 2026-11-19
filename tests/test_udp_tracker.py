"""UDP tracker announces (BEP 15) against a fake tracker on 127.0.0.1."""
import asyncio
import struct

from yap_torrent.protocol import tracker as tracker_module
from yap_torrent.protocol import udp_tracker
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.protocol.tracker import make_announce
from yap_torrent.protocol.udp_tracker import make_udp_announce

INFO_HASH = bytes(range(20))
PEER_ID = b"-PY0001-111111111111"
FAST = (0.2, 0.4)  # retry schedule for tests

PEERS = [PeerInfo("10.0.0.1", 6881), PeerInfo("192.168.1.20", 51413)]


def _compact(peers) -> bytes:
	return b"".join(bytes(int(x) for x in p.host.split(".")) + p.port.to_bytes(2, "big") for p in peers)


class FakeTracker(asyncio.DatagramProtocol):
	"""Speaks just enough BEP 15. `drop` ignores the first N packets; `error` fails announces."""

	CONNECTION_ID = 0x1122334455667788

	def __init__(self, drop: int = 0, error: str = "", silent: bool = False):
		self.drop = drop
		self.error = error
		self.silent = silent
		self.announces = []
		self.transport = None

	def connection_made(self, transport):
		self.transport = transport

	def datagram_received(self, data, addr):
		if self.silent:
			return
		if self.drop:
			self.drop -= 1
			return
		if len(data) == 16:
			protocol_id, action, tid = struct.unpack(">qii", data)
			assert protocol_id == udp_tracker.PROTOCOL_ID and action == 0
			self.transport.sendto(struct.pack(">iiq", 0, tid, self.CONNECTION_ID), addr)
		elif len(data) == 98:
			fields = struct.unpack(">qii20s20sqqqiIIiH", data)
			conn, action, tid = fields[:3]
			assert conn == self.CONNECTION_ID and action == 1
			self.announces.append(fields)
			if self.error:
				self.transport.sendto(struct.pack(">ii", 3, tid) + self.error.encode(), addr)
				return
			# a stale reply first (wrong transaction id) — must be ignored
			self.transport.sendto(struct.pack(">iiiii", 1, tid ^ 1, 1, 1, 1), addr)
			self.transport.sendto(struct.pack(">iiiii", 1, tid, 1800, 7, 3) + _compact(PEERS), addr)


async def _serve(**kwargs):
	loop = asyncio.get_running_loop()
	transport, fake = await loop.create_datagram_endpoint(
		lambda: FakeTracker(**kwargs), local_addr=("127.0.0.1", 0))
	return transport, fake, f"udp://127.0.0.1:{transport.get_extra_info('sockname')[1]}/announce"


def test_announce_returns_peers_and_counts():
	async def run():
		transport, fake, url = await _serve()
		try:
			result = await make_udp_announce(url, INFO_HASH, PEER_ID, downloaded=5, uploaded=0,
			                                 left=1000, port=6889, event="started", retry_waits=FAST)
		finally:
			transport.close()

		assert result is not None
		assert result.peers == tuple(PEERS)
		assert (result.interval, result.complete, result.incomplete) == (1800, 3, 7)
		assert result.failure_reason == ""

		(_, _, _, info_hash, peer_id, downloaded, left, uploaded, event, ip, _key, num_want, port), = fake.announces
		assert (info_hash, peer_id) == (INFO_HASH, PEER_ID)
		assert (downloaded, left, uploaded) == (5, 1000, 0)
		assert event == 2 and ip == 0 and num_want == 50 and port == 6889

	asyncio.run(run())


def test_a_lost_packet_is_retransmitted():
	async def run():
		transport, fake, url = await _serve(drop=1)
		try:
			result = await make_udp_announce(url, INFO_HASH, PEER_ID, retry_waits=FAST)
		finally:
			transport.close()
		assert result is not None and result.peers == tuple(PEERS)

	asyncio.run(run())


def test_an_error_reply_becomes_a_failure_reason():
	async def run():
		transport, fake, url = await _serve(error="torrent not registered")
		try:
			result = await make_udp_announce(url, INFO_HASH, PEER_ID, retry_waits=FAST)
		finally:
			transport.close()
		assert result is not None
		assert result.failure_reason == "torrent not registered"
		assert result.peers == ()

	asyncio.run(run())


def test_a_silent_tracker_times_out():
	async def run():
		transport, fake, url = await _serve(silent=True)
		try:
			return await make_udp_announce(url, INFO_HASH, PEER_ID, retry_waits=FAST)
		finally:
			transport.close()

	assert asyncio.run(run()) is None


def test_a_closed_port_fails_without_raising():
	async def run():
		transport, fake, url = await _serve()
		transport.close()  # nothing listens there any more
		return await make_udp_announce(url, INFO_HASH, PEER_ID, retry_waits=FAST)

	assert asyncio.run(run()) is None


def test_a_bad_address_fails_without_raising():
	async def run():
		assert await make_udp_announce("udp://no-port.example/announce", INFO_HASH, PEER_ID,
		                               retry_waits=FAST) is None
		assert await make_udp_announce("udp://host:notaport/announce", INFO_HASH, PEER_ID,
		                               retry_waits=FAST) is None

	asyncio.run(run())


def test_make_announce_routes_udp_urls(monkeypatch):
	async def run():
		transport, fake, url = await _serve()
		try:
			return await make_announce(url, INFO_HASH, peer_id=PEER_ID, port=6889, event="completed")
		finally:
			transport.close()

	calls = []

	async def fake_udp(*args, **kwargs):
		calls.append((args, kwargs))
		return await make_udp_announce(*args, **kwargs, retry_waits=FAST)

	monkeypatch.setattr(tracker_module, "make_udp_announce", fake_udp)
	result = asyncio.run(run())
	assert result is not None and result.peers == tuple(PEERS)
	assert calls and calls[0][1]["event"] == "completed" and calls[0][1]["port"] == 6889


def test_unknown_tracker_schemes_are_skipped():
	async def run():
		return await make_announce("wss://tracker.example/announce", INFO_HASH, peer_id=PEER_ID)

	assert asyncio.run(run()) is None
