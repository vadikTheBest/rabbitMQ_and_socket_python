import pyDes
import rsa
(pubkey, privkey) = rsa.newkeys(512)
crypto = rsa.encrypt(b"DESCRYPT", pubkey)
#расшифровываем
rsakey = rsa.decrypt(crypto, privkey)
k = pyDes.des(rsakey, pyDes.CBC, "\0\0\0\0\0\0\0\0", pad=None, padmode=pyDes.PAD_PKCS5)

