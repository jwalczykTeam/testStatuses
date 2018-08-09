'''
Created on Oct 13, 2016

@author: sasha
'''


from Crypto.Cipher import AES


def encryption(item, key, iv, protocol):
    """
    Method to encrypt rdd using AES
    item        is some form of traversable either dict, list, or tuple
    key         key used for encyption - 16 bytes for 128, 32 bytes for 256
    iv          initalization vector for encryption
    protocol    either 'encrypt' or 'decrypt'
    """

    def is_list(d):
        return isinstance(d, list) and not isinstance(d, basestring)

    def is_tuple(d):
        return isinstance(d, list) and not isinstance(d, basestring)

    def is_dict(d):
        return isinstance(d, dict)

    def is_traversable(d):
        return is_list(d) or is_tuple(d) or is_dict(d)

    def is_string(d):
        return isinstance(d, basestring)

    def crypter(x):
        if is_string(x):
            cipher = AES.new(key, AES.MODE_CFB, iv)
                        
            if protocol == 'encrypt':
                msg = getattr(cipher, protocol)(x.encode('utf-8')).decode('latin-1')
            elif protocol == 'decrypt':
                msg = getattr(cipher, protocol)(x.encode('latin-1')).decode('utf-8')
            else:
                raise "Protocol {} not supported by crypter".format(protocol)
            
            return msg
        elif is_traversable(x):
            return encryption(x, key, iv, protocol)
        else:
            return x

    if is_list(item):
        return [crypter(_) for _ in item]
    elif is_tuple(item):
        return tuple(crypter(_) for _ in item)
    else:
        return {key:crypter(item[key]) for key in item}


KEY = "super secret keysuper secret key"
IV = "super secret iv6"

def encrypt(doc): 
    return encryption(doc, key=KEY, iv=IV, protocol='encrypt') 
