import asyncio
import json
import os
import time
import random

HEARTBEAT_INTERVAL = 2
ELECTION_TIMEOUT_MIN = 6
ELECTION_TIMEOUT_MAX = 9
ELECTION_DEBOUNCE = 1.5


class Node:
    def __init__(self):
        self.node_id = int(os.getenv("NODE_ID"))
        self.port = int(os.getenv("NODE_PORT"))
        self.role = "follower"
        self.term = 0
        self.voted_for = None
        self.leader = None
        self.heartbeat_time = time.time()
        self.vote_count = 0
        self.host = "0.0.0.0"
        self.peers = []
        self.last_election_time = 0
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        peer_list = os.getenv("PEERS", "")
        if peer_list:
            for peer in peer_list.split(","):
                host, port = peer.split(":")
                self.peers.append(
                    (host, int(port))
                )

    def get_majority(self):
        total_nodes = len(self.peers) + 1
        return (total_nodes // 2) + 1

    async def send(self, host, port, message):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write((json.dumps(message) + "\n").encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    async def send_to_all(self, message):
        jobs = []
        for host, port in self.peers:
            jobs.append(self.send(host, port, message))
        if jobs:
            await asyncio.gather(*jobs)

    # Leader keeps sending heartbeats
    async def heartbeat_loop(self):
        while True:
            if self.role == "leader":
                self.heartbeat_time = time.time()
                heartbeat = {
                    "type": "HEARTBEAT",
                    "leader_id": self.node_id,
                    "term": self.term
                }
                await self.send_to_all(heartbeat)
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    # Start election if heartbeat is missing
    async def election_loop(self):
        while True:
            if self.role != "leader":
                elapsed = time.time() - self.heartbeat_time
                time_since_election = time.time() - self.last_election_time
                if elapsed > self.election_timeout and time_since_election > ELECTION_DEBOUNCE:
                    await self.start_election()
            else:
                self.last_election_time = time.time()
            await asyncio.sleep(1)

    async def start_election(self):
        if self.role == "leader":
            return
        self.role = "candidate"
        self.term += 1
        self.voted_for = self.node_id
        self.vote_count = 1
        self.heartbeat_time = time.time()
        self.last_election_time = time.time()
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        print(f"[Node {self.node_id}] Election started (term={self.term})")
        vote_request = {
            "type": "VOTE_REQUEST",
            "candidate_id": self.node_id,
            "candidate_host": "localhost",
            "candidate_port": self.port,
            "term": self.term
        }
        await self.send_to_all(vote_request)

    async def make_leader(self):
        self.role = "leader"
        self.leader = self.node_id
        self.heartbeat_time = time.time()
        self.last_election_time = time.time()
        print(f"[Node {self.node_id}] *** LEADER ELECTED ***")

    async def process_vote_request(self, message):
        request_term = message["term"]
        candidate_id = message["candidate_id"]
        candidate_host = message["candidate_host"]
        candidate_port = message["candidate_port"]
        grant_vote = False
        if request_term > self.term:
            self.term = request_term
            self.voted_for = None
            if self.role != "leader":
                self.role = "follower"
                self.heartbeat_time = time.time()
        if request_term == self.term and (self.voted_for is None or self.voted_for == candidate_id):
            grant_vote = True
            self.voted_for = candidate_id
        reply = {
            "type": "VOTE_RESPONSE",
            "term": self.term,
            "vote_granted": grant_vote
        }
        await self.send(candidate_host, candidate_port, reply)

    async def process_vote_response(self, message):
        if self.role != "candidate":
            return
        if message["term"] > self.term:
            self.term = message["term"]
            self.role = "follower"
            self.voted_for = None
            return
        if message["vote_granted"]:
            self.vote_count += 1
            print(f"[Node {self.node_id}] Votes={self.vote_count}")
            if self.vote_count >= self.get_majority():
                await self.make_leader()

    async def process_heartbeat(self, message):
        leader_term = message["term"]
        if leader_term >= self.term:
            self.term = leader_term
            self.role = "follower"
            self.leader = message["leader_id"]
            self.heartbeat_time = time.time()

    async def process_message(self, message):
        msg_type = message["type"]
        if msg_type == "HEARTBEAT":
            await self.process_heartbeat(message)
        elif msg_type == "VOTE_REQUEST":
            await self.process_vote_request(message)
        elif msg_type == "VOTE_RESPONSE":
            await self.process_vote_response(message)

    async def handle_connection(self, reader, writer):
        try:
            data = await reader.readline()
            if data:
                message = json.loads(data.decode())
                await self.process_message(message)
        except Exception as err:
            print(f"[Node {self.node_id}] {err}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        print(f"[Node {self.node_id}] Listening on {self.port}")
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.election_loop())
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    node = Node()
    asyncio.run(node.start())