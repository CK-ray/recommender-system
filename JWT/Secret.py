import secrets

secret_key = secrets.token_hex(32)
print(secret_key)

from cryptography.fernet import Fernet

encryption_key = Fernet.generate_key()
print(encryption_key.decode())