import asyncio
import json
import os

TRANSACTION_INTERVAL = 5

async def send_transaction(host, port, transaction_id, payload):
    try:
        reader, writer = await asyncio.open_connection(host, port)
        message = {
            "type": "TRANSACTION",
            "transaction_id": transaction_id,
            "payload": payload
        }
        writer.write((json.dumps(message) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        print(f"[Client] Sent {transaction_id}")
    except Exception as err:
        print(f"[Client] Error: {err}")

async def transaction_generator(host, port):
    transaction_count = 1
    while True:
        transaction_id = f"TX{transaction_count:03d}"
        payload = f"Payment {transaction_count}"
        await send_transaction(host, port, transaction_id, payload)
        transaction_count += 1
        await asyncio.sleep(TRANSACTION_INTERVAL)

async def main():
    host = os.getenv("CLIENT_TARGET_HOST", "localhost")
    port = int(os.getenv("CLIENT_TARGET_PORT", "8001"))
    await transaction_generator(host, port)

if __name__ == "__main__":
    asyncio.run(main())