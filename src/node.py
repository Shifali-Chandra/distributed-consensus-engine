import asyncio
import json
import os
import time
import random

from crypto_utils import sign_message, verify_signature

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
        self.leader_host = None
        self.leader_port = None
        self.heartbeat_time = time.time()
        self.vote_count = 0
        self.host="localhost"
        self.peers = []
        self.last_election_time = 0
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self.ledger = []
        self.promise_counts = {}
        self.accepted_counts = {}
        self.pending_transactions = {}
        self.accept_sent = set()
        self.promise_complete = set()
        self.mode = os.getenv("CONSENSUS_MODE", "PAXOS")
        self.pbft_prepare_counts = {}
        self.pbft_commit_counts = {}
        self.pbft_transactions = {}
        self.pbft_prepare_sent = set()
        self.pbft_commit_sent = set()
        self.pbft_prepare_complete=set()
        self.pbft_completed=set()
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
                    "leader_host": "localhost",
                    "leader_port": self.port,
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
        self.role="leader"
        self.leader=self.node_id
        self.leader_host="localhost"
        self.leader_port=self.port
        self.heartbeat_time=time.time()
        self.last_election_time=time.time()
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

    async def process_heartbeat(self,message):
        if message["leader_id"]==self.node_id:
            return
        leader_term=message["term"]
        if leader_term>=self.term:
            self.term=leader_term
            self.role="follower"
            self.leader=message["leader_id"]
            self.leader_host=message.get("leader_host","localhost")
            self.leader_port=message.get("leader_port",8000+self.leader)
            self.heartbeat_time=time.time()

    async def append_transaction(self, transaction):
        self.ledger.append(transaction)
        with open(f"ledger_node_{self.node_id}.log", "a") as f:
            f.write(json.dumps(transaction) + "\n")
        print(f"[Node {self.node_id}] Transaction committed: {transaction['transaction_id']}")

    async def forward_to_leader(self, message):
        if self.leader_host and self.leader_port:
            await self.send(self.leader_host, self.leader_port, message)

    async def process_transaction(self, message):
        if self.role=="leader" and self.leader==self.node_id:
            transaction_id = message.get("transaction_id", "UNKNOWN")
            payload = message.get("payload", "")
            transaction = {
                "transaction_id": transaction_id,
                "payload": payload
            }
            if self.mode == "PBFT":
                await self.start_pbft(transaction)
            else:
                await self.start_paxos(transaction)
        else:
            transaction_id = message.get("transaction_id", "UNKNOWN")
            if self.leader:
                print(f"[Node {self.node_id}] Forwarding {transaction_id} to leader Node {self.leader}")
                await self.forward_to_leader(message)
            else:
                print(f"[Node {self.node_id}] No leader available for {transaction_id}")

    async def start_paxos(self, transaction):
        transaction_id = transaction.get("transaction_id", "UNKNOWN")
        self.pending_transactions[transaction_id] = transaction
        self.promise_counts[transaction_id] = self.promise_counts.get(transaction_id, 0) + 1
        self.accepted_counts[transaction_id] = self.accepted_counts.get(transaction_id, 0)
        self.accept_sent.discard(transaction_id)
        prepare_msg = {
            "type": "PREPARE",
            "transaction": transaction
        }
        print(f"[Node {self.node_id}] PREPARE {transaction_id}")  
        await self.send_to_all(prepare_msg)

    async def process_prepare(self, message):
        try:
            transaction = message.get("transaction", {})
            transaction_id = transaction.get("transaction_id", "UNKNOWN")
            promise_msg = {
                "type": "PROMISE",
                "transaction_id": transaction_id
            }
            if self.leader and self.leader_port:
                print(f"[Node {self.node_id}] Sending PROMISE for {transaction_id}")
                await self.send(self.leader_host, self.leader_port, promise_msg)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def process_promise(self, message):
        if self.role != "leader":
            return
        try:
            transaction_id = message.get("transaction_id", "UNKNOWN")
            if transaction_id in self.promise_complete:
                return
            if transaction_id not in self.pending_transactions:
                return
            self.promise_counts[transaction_id] = self.promise_counts.get(transaction_id, 0) + 1
            print(f"[Node {self.node_id}] PROMISE count={self.promise_counts[transaction_id]}")
            if self.promise_counts[transaction_id] >= self.get_majority() and transaction_id not in self.accept_sent:
                self.promise_complete.add(transaction_id)
                self.accept_sent.add(transaction_id)
                transaction = self.pending_transactions.get(transaction_id)
                if transaction is None:
                    return
                self.accepted_counts[transaction_id] = 1
                accept_msg = {
                    "type": "ACCEPT",
                    "transaction": transaction
                }
                print(f"[Node {self.node_id}] ACCEPT {transaction_id}")
                await self.send_to_all(accept_msg)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def process_accept(self, message):
        try:
            transaction = message.get("transaction", {})
            transaction_id = transaction.get("transaction_id", "UNKNOWN")
            accepted_msg = {
                "type": "ACCEPTED",
                "transaction_id": transaction_id
            }
            if self.leader and self.leader_port:
                print(f"[Node {self.node_id}] Sending ACCEPTED for {transaction_id}")
                await self.send(self.leader_host, self.leader_port, accepted_msg)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def process_accepted(self, message):
        if self.role != "leader":
            return
        try:
            transaction_id = message.get("transaction_id", "UNKNOWN")
            if transaction_id not in self.pending_transactions:
                return
            self.accepted_counts[transaction_id] = self.accepted_counts.get(transaction_id, 0) + 1
            print(f"[Node {self.node_id}] ACCEPTED count={self.accepted_counts[transaction_id]}")
            if self.accepted_counts[transaction_id] >= self.get_majority():
                transaction = self.pending_transactions.get(transaction_id)
                if transaction is None:
                    return
                await self.append_transaction(transaction)
                print(f"[Node {self.node_id}] Paxos consensus reached for {transaction_id}")
                self.pending_transactions.pop(transaction_id, None)
                self.promise_counts.pop(transaction_id, None)
                self.accepted_counts.pop(transaction_id, None)
                self.accept_sent.discard(transaction_id)
                self.promise_complete.discard(transaction_id)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def start_pbft(self, transaction):
        transaction_id = transaction.get("transaction_id", "UNKNOWN")
        self.pbft_transactions[transaction_id] = transaction
        signature = sign_message(self.node_id, transaction)
        pre_prepare_msg = {
            "type": "PRE_PREPARE",
            "transaction": transaction,
            "sender": self.node_id,
            "signature": signature
        }
        print(f"[Node {self.node_id}] PRE-PREPARE {transaction_id}")
        await self.send_to_all(pre_prepare_msg)

    async def process_pre_prepare(self, message):
        try:
            transaction = message.get("transaction", {})
            transaction_id = transaction.get("transaction_id", "UNKNOWN")
            sender = message.get("sender")
            signature = message.get("signature")
            if sender is None or signature is None:
                return
            if not verify_signature(sender, transaction, signature):
                print(f"[Node {self.node_id}] PRE-PREPARE signature invalid for {transaction_id}")
                return
            self.pbft_transactions[transaction_id] = transaction
            if transaction_id in self.pbft_prepare_sent:
                return
            self.pbft_prepare_sent.add(transaction_id)
            prepare_payload = {"transaction_id": transaction_id}
            prepare_signature = sign_message(self.node_id, prepare_payload)
            prepare_msg = {
                "type": "PBFT_PREPARE",
                "transaction_id": transaction_id,
                "sender": self.node_id,
                "signature": prepare_signature
            }
            print(f"[Node {self.node_id}] PREPARE {transaction_id}")
            await self.send_to_all(prepare_msg)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def process_pbft_prepare(self,message):
        try:
            transaction_id=message.get("transaction_id","UNKNOWN")
            sender=message.get("sender")
            signature=message.get("signature")
            if sender is None or signature is None:
                return
            if transaction_id in self.pbft_prepare_complete:
                return
            if not verify_signature(sender,{"transaction_id":transaction_id},signature):
                print(f"[Node {self.node_id}] PBFT_PREPARE signature invalid for {transaction_id}")
                return
            self.pbft_prepare_counts[transaction_id]=self.pbft_prepare_counts.get(transaction_id,0)+1
            count=self.pbft_prepare_counts[transaction_id]
            print(f"[Node {self.node_id}] PREPARE count={count} for {transaction_id}")
            if count>=3 and transaction_id not in self.pbft_commit_sent:
                self.pbft_prepare_complete.add(transaction_id)
                self.pbft_commit_sent.add(transaction_id)
                commit_payload={"transaction_id":transaction_id}
                commit_signature=sign_message(self.node_id,commit_payload)
                commit_msg={
                    "type":"PBFT_COMMIT",
                    "transaction_id":transaction_id,
                    "sender":self.node_id,
                    "signature":commit_signature
                }
                print(f"[Node {self.node_id}] COMMIT {transaction_id}")
                await self.send_to_all(commit_msg)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def process_pbft_commit(self,message):
        try:
            transaction_id=message.get("transaction_id","UNKNOWN")
            sender=message.get("sender")
            signature=message.get("signature")
            if sender is None or signature is None:
                return
            if transaction_id in self.pbft_completed:
                return
            if not verify_signature(sender,{"transaction_id":transaction_id},signature):
                print(f"[Node {self.node_id}] PBFT_COMMIT signature invalid for {transaction_id}")
                return
            self.pbft_commit_counts[transaction_id]=self.pbft_commit_counts.get(transaction_id,0)+1
            count=self.pbft_commit_counts[transaction_id]
            print(f"[Node {self.node_id}] COMMIT count={count} for {transaction_id}")
            if count>=3:
                self.pbft_completed.add(transaction_id)
                transaction=self.pbft_transactions.get(transaction_id)
                if transaction is None:
                    return
                await self.append_transaction(transaction)
                print(f"[Node {self.node_id}] PBFT consensus reached for {transaction_id}")
                self.pbft_transactions.pop(transaction_id,None)
                self.pbft_prepare_counts.pop(transaction_id,None)
                self.pbft_commit_counts.pop(transaction_id,None)
                self.pbft_prepare_sent.discard(transaction_id)
                self.pbft_commit_sent.discard(transaction_id)
                self.pbft_prepare_complete.discard(transaction_id)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")

    async def process_message(self, message):
        msg_type = message["type"]
        if msg_type == "HEARTBEAT":
            await self.process_heartbeat(message)
        elif msg_type == "VOTE_REQUEST":
            await self.process_vote_request(message)
        elif msg_type == "VOTE_RESPONSE":
            await self.process_vote_response(message)
        elif msg_type == "TRANSACTION":
            await self.process_transaction(message)
        elif msg_type == "PREPARE":
            await self.process_prepare(message)
        elif msg_type == "PROMISE":
            await self.process_promise(message)
        elif msg_type == "ACCEPT":
            await self.process_accept(message)
        elif msg_type == "ACCEPTED":
            await self.process_accepted(message)
        elif msg_type == "PRE_PREPARE":
            await self.process_pre_prepare(message)
        elif msg_type == "PBFT_PREPARE":
            await self.process_pbft_prepare(message)
        elif msg_type == "PBFT_COMMIT":
            await self.process_pbft_commit(message)

    async def handle_connection(self, reader, writer):
        try:
            data = await reader.readline()
            if data:
                message = json.loads(data.decode())
                await self.process_message(message)
        except Exception as err:
            print(f"[Node {self.node_id}] Error: {err}")
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