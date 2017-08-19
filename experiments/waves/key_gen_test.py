import hashlib

import axolotl_curve25519 as curve
import base58
import pyblake2
import sha3

def hashChain(noncedSecret):
    b = pyblake2.blake2b(noncedSecret, digest_size=32).digest()
    return sha3.keccak_256(b).digest()

seed = "foo0"
print(base58.b58encode(seed))

seedHash = hashChain('\0\0\0\0' + seed)
accountSeedHash = hashlib.sha256(seedHash).digest()

private_key = curve.generatePrivateKey(accountSeedHash)
public_key = curve.generatePublicKey(private_key)

unhashedAddress = chr(1) + "L" + hashChain(public_key)[0:20]
addressHash = hashChain(unhashedAddress)[0:4]
address = base58.b58encode(unhashedAddress + addressHash)

print("seed         : %s" % seed)
print("public key   : %s" % base58.b58encode(public_key))
print("private key  : %s" % base58.b58encode(private_key))
print("address      : %s" % address)
print("-" * 120)
