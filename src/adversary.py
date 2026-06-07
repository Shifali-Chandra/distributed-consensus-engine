import asyncio
import os

from node import Node

VALID_MODES = {"DROP_COMMIT", "INVALID_SIGNATURE", "EQUIVOCATE"}


class AdversaryNode(Node):
    def __init__(self):
        self.byzantine_mode = os.getenv("BYZANTINE_MODE", "").upper()
        super().__init__()
        if self.byzantine_mode and self.byzantine_mode not in VALID_MODES:
            print(f"[Adversary {self.node_id}] Unknown BYZANTINE_MODE '{self.byzantine_mode}', behaving normally")
            self.byzantine_mode = ""
        else:
            print(f"[Adversary {self.node_id}] Mode: {self.byzantine_mode or 'NORMAL'}")

    async def start_election(self):
        print(f"[Adversary {self.node_id}] Not participating in leader election")
        return

    async def process_pre_prepare(self, message):
        transaction = message.get("transaction", {})
        transaction_id = transaction.get("transaction_id", "UNKNOWN")
        if self.byzantine_mode == "INVALID_SIGNATURE":
            tampered = dict(message)
            tampered["signature"] = "fake_signature"
            print(f"[Adversary {self.node_id}] Sending invalid signature for {transaction_id}")
            return tampered
        return message

    async def process_pbft_prepare(self, message):
        transaction = message.get("transaction", {})
        transaction_id = transaction.get("transaction_id", "UNKNOWN")
        if self.byzantine_mode == "EQUIVOCATE":
            print(f"[Adversary {self.node_id}] Equivocating PREPARE for {transaction_id}")
            real_msg = dict(message)
            fake_msg = dict(message)
            fake_msg["transaction"] = {**transaction, "transaction_id": f"{transaction_id}_FAKE"}
            half = len(self.peers) // 2
            group_a = self.peers[:half]
            group_b = self.peers[half:]
            tasks = []
            for host, port in group_a:
                tasks.append(self.send(host, port, real_msg))
            for host, port in group_b:
                tasks.append(self.send(host, port, fake_msg))
            if tasks:
                await asyncio.gather(*tasks)
            return [real_msg, fake_msg]
        return message

    async def process_pbft_commit(self, message):
        transaction = message.get("transaction", {})
        transaction_id = transaction.get("transaction_id", "UNKNOWN")
        if self.byzantine_mode == "DROP_COMMIT":
            print(f"[Adversary {self.node_id}] Dropping COMMIT for {transaction_id}")
            return None
        return message


if __name__ == "__main__":
    node = AdversaryNode()
    asyncio.run(node.start())
