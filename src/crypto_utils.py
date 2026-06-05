import json
import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

KEYS_DIR = "keys"
KEY_SIZE = 2048


def _private_key_path(node_id):
    return os.path.join(KEYS_DIR, f"node_{node_id}_private.pem")


def _public_key_path(node_id):
    return os.path.join(KEYS_DIR, f"node_{node_id}_public.pem")


def generate_key_pair(node_id):
    try:
        os.makedirs(KEYS_DIR, exist_ok=True)
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=KEY_SIZE)
        public_key = private_key.public_key()

        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        with open(_private_key_path(node_id), "wb") as f:
            f.write(private_pem)
        with open(_public_key_path(node_id), "wb") as f:
            f.write(public_pem)

        print(f"[Crypto] Keys generated for Node {node_id}")
    except Exception as err:
        print(f"[Crypto] Key generation failed for Node {node_id}: {err}")


def load_private_key(node_id):
    try:
        with open(_private_key_path(node_id), "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)
    except Exception as err:
        print(f"[Crypto] Failed to load private key for Node {node_id}: {err}")
        return None


def load_public_key(node_id):
    try:
        with open(_public_key_path(node_id), "rb") as f:
            return serialization.load_pem_public_key(f.read())
    except Exception as err:
        print(f"[Crypto] Failed to load public key for Node {node_id}: {err}")
        return None


def _message_bytes(message):
    # Deterministic JSON encoding: sorted keys keep signatures reproducible
    return json.dumps(message, sort_keys=True).encode()


def sign_message(node_id, message):
    try:
        private_key = load_private_key(node_id)
        if private_key is None:
            return None
        signature = private_key.sign(
            _message_bytes(message),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return signature.hex()
    except Exception as err:
        print(f"[Crypto] Signing failed for Node {node_id}: {err}")
        return None


def verify_signature(node_id, message, signature):
    try:
        public_key = load_public_key(node_id)
        if public_key is None:
            return False
        signature_bytes = bytes.fromhex(signature)
        public_key.verify(
            signature_bytes,
            _message_bytes(message),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return True
    except InvalidSignature:
        return False
    except Exception as err:
        print(f"[Crypto] Verification error for Node {node_id}: {err}")
        return False


if __name__ == "__main__":
    generate_key_pair(1)

    message = {
        "transaction_id": "TX001",
        "payload": "Payment 100",
    }

    signature = sign_message(1, message)
    print(f"[Crypto] Signature: {signature}")
    print(f"[Crypto] Verify (valid): {verify_signature(1, message, signature)}")
    print(f"[Crypto] Verify (tampered): {verify_signature(1, {**message, 'payload': 'Payment 999'}, signature)}")
