"""UDP tracker client, BEP 15: https://bittorrent.org/beps/bep_0015.html

Two round trips per announce: CONNECT gets a connection id, ANNOUNCE uses it. Each request
is retransmitted on a short schedule (BEP 15's own 15 * 2^n seconds would park the announce
loop for up to an hour on a dead tracker). The reply is wrapped in the same
TrackerAnnounceResponse an HTTP tracker produces, so the announce system can't tell them apart.
"""
import asyncio
import logging
import random
import socket
import struct
from typing import Optional, Sequence
from urllib.parse import urlsplit

from yap_torrent.protocol.structures import TrackerAnnounceResponse

logger = logging.getLogger(__name__)

PROTOCOL_ID = 0x41727101980  # magic constant every CONNECT starts with

ACTION_CONNECT = 0
ACTION_ANNOUNCE = 1
ACTION_ERROR = 3

EVENTS = {"": 0, "completed": 1, "started": 2, "stopped": 3}

# seconds to wait for each try of one request; worst case for a silent tracker is
# sum() for CONNECT plus sum() for ANNOUNCE
RETRY_WAITS: Sequence[float] = (3.0, 6.0)

_HEADER = struct.Struct(">ii")  # action, transaction_id — the start of every reply
_CONNECT_REQUEST = struct.Struct(">qii")  # protocol_id, action, transaction_id
_CONNECT_REPLY = struct.Struct(">iiq")  # action, transaction_id, connection_id
_ANNOUNCE_REQUEST = struct.Struct(">qii20s20sqqqiIIiH")
_ANNOUNCE_REPLY = struct.Struct(">iiiii")  # action, transaction_id, interval, leechers, seeders

# one key per run, so a tracker can recognise us across an IP change
_KEY = random.getrandbits(32)


class TrackerErrorReply(Exception):
	"""The tracker answered with an ERROR action; the message is its failure reason."""


class _Protocol(asyncio.DatagramProtocol):
	def __init__(self):
		self._queue: asyncio.Queue = asyncio.Queue()

	def datagram_received(self, data: bytes, addr) -> None:
		self._queue.put_nowait(data)

	def error_received(self, exc: Exception) -> None:
		# e.g. ICMP port unreachable on a connected socket: fail now, not after the timeout
		self._queue.put_nowait(exc)

	async def receive(self, timeout: float) -> bytes:
		item = await asyncio.wait_for(self._queue.get(), timeout)
		if isinstance(item, Exception):
			raise item
		return item


def _new_transaction_id() -> int:
	return random.getrandbits(31)  # fits the signed int32 field


async def _transact(transport: asyncio.DatagramTransport, protocol: _Protocol,
                    build, action: int, min_size: int, retry_waits: Sequence[float]) -> bytes:
	"""Send `build(transaction_id)` until a matching reply of `action` arrives."""
	loop = asyncio.get_running_loop()
	for wait in retry_waits:
		transaction_id = _new_transaction_id()
		transport.sendto(build(transaction_id))
		deadline = loop.time() + wait
		while (remaining := deadline - loop.time()) > 0:
			try:
				data = await protocol.receive(remaining)
			except TimeoutError:
				break
			if len(data) < _HEADER.size:
				continue
			reply_action, reply_tid = _HEADER.unpack_from(data)
			if reply_tid != transaction_id:
				continue  # a late answer to an earlier try
			if reply_action == ACTION_ERROR:
				raise TrackerErrorReply(data[_HEADER.size:].decode("utf-8", "replace"))
			if reply_action == action and len(data) >= min_size:
				return data
	raise TimeoutError(f"no reply after {len(retry_waits)} tries")


async def make_udp_announce(
		announce: str,
		info_hash: bytes,
		peer_id: bytes,
		downloaded: int = 0,
		uploaded: int = 0,
		left: int = 0,
		port: int = 6881,
		event: str = "",
		num_want: int = 50,
		retry_waits: Sequence[float] = RETRY_WAITS,
) -> Optional[TrackerAnnounceResponse]:
	url = urlsplit(announce)
	try:
		host, tracker_port = url.hostname, url.port
	except ValueError:
		host = tracker_port = None
	if not host or not tracker_port:
		logger.warning("Bad UDP tracker address %s", announce)
		return None

	loop = asyncio.get_running_loop()
	transport = None
	try:
		# IPv4 only: an IPv6 tracker answers with 18-byte peers, which PeerInfo can't read
		transport, protocol = await loop.create_datagram_endpoint(
			_Protocol, remote_addr=(host, tracker_port), family=socket.AF_INET)

		reply = await _transact(
			transport, protocol,
			lambda tid: _CONNECT_REQUEST.pack(PROTOCOL_ID, ACTION_CONNECT, tid),
			ACTION_CONNECT, _CONNECT_REPLY.size, retry_waits)
		_, _, connection_id = _CONNECT_REPLY.unpack_from(reply)

		reply = await _transact(
			transport, protocol,
			lambda tid: _ANNOUNCE_REQUEST.pack(
				connection_id, ACTION_ANNOUNCE, tid, info_hash, peer_id,
				max(downloaded, 0), max(left, 0), max(uploaded, 0), EVENTS.get(event, 0),
				0,  # IP address: 0 = the address the packet came from
				_KEY, num_want, port),
			ACTION_ANNOUNCE, _ANNOUNCE_REPLY.size, retry_waits)
	except TrackerErrorReply as ex:
		return TrackerAnnounceResponse({"failure reason": str(ex).encode("utf-8")}, 1)
	except TimeoutError:
		logger.warning("UDP tracker %s did not answer", announce)
		return None
	except OSError as ex:  # DNS failure, port unreachable, no route, ...
		logger.warning("Connection error on UDP announce to %s: %s", announce, ex)
		return None
	finally:
		if transport is not None:
			transport.close()

	_, _, interval, leechers, seeders = _ANNOUNCE_REPLY.unpack_from(reply)
	peers = reply[_ANNOUNCE_REPLY.size:]
	peers = peers[:len(peers) - len(peers) % 6]  # whole 6-byte (IPv4 + port) entries only
	return TrackerAnnounceResponse({
		"interval": interval,
		"complete": seeders,
		"incomplete": leechers,
		"peers": peers,
	}, 1)
