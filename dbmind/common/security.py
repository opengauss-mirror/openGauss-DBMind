# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
"""Avoid leaking plain-text passwords."""
import base64
import hmac
import os
import re
import secrets
import string
import subprocess

from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from Crypto.Util.Padding import pad, unpad

from dbmind.common import utils
from dbmind.common.exceptions import CertCheckException

lowercase_pattern = re.compile(r'[a-z]')
uppercase_pattern = re.compile(r'[A-Z]')
digit_pattern = re.compile(r'\d')
special_char = '[~`!@#$%^&*()-_+\\|{};:,<.>/?]'
ENCRYPTION_ALGORITHM_DETAILS = {
    'id-ecpublickey': {'min_length': 224, 'recommended_length': 256},
    'rsaencryption': {'min_length': 2048, 'recommended_length': 3072},
    'dsaencryption': {'min_length': 2048, 'recommended_length': 3072},
}


def check_password_strength(password, username=None, is_ssl_keyfile_password=False):
    if len(password) < 8:
        return False

    if not is_ssl_keyfile_password and password == username:
        return False

    password_strength = 0
    password_strength += int(bool(lowercase_pattern.search(password)))
    password_strength += int(bool(uppercase_pattern.search(password)))
    password_strength += int(bool(digit_pattern.search(password)))
    password_strength += int(any(x in special_char for x in password))
    return 2 <= password_strength <= 4


def safe_random_string(length):
    """Used to generate a fixed-length random
    string which is used in the security and cryptography."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def generate_an_iv() -> str:
    """Generate and return an initialization vector for AES."""
    return safe_random_string(16)


def encrypt(s1: str, s2: str, iv: str, pt: str) -> str:
    """Encrypt a series of plain text with two strings.
    :param s1: string #1
    :param s2: string #2
    :param iv: initialization vector, used by AES256-CBC
    :param pt: plain text
    :return: cipher text
    """
    if pt == '':
        return ''
    nb = 16  # the number of block including cipher and plain text
    h = hmac.new(s1.encode(), s2.encode(), digestmod='sha256')
    master_key = h.hexdigest()[:32].encode()  # 32 bytes means AES256
    cipher = AES.new(master_key, AES.MODE_CBC, iv.encode())
    pt = pt.encode()
    ct = cipher.encrypt(pad(pt, nb))
    return base64.b64encode(ct).decode()


def decrypt(s1: str, s2: str, iv: str, ct: str) -> str:
    """Decrypt a series of cipher text with two strings.
    :param s1: string #1
    :param s2: string #2
    :param iv: initialization vector, used by AES256-CBC
    :param ct: cipher text
    :return: plain text
    """
    if ct == '':
        return ''
    nb = 16  # the number of block including cipher and plain text
    h = hmac.new(s1.encode(), s2.encode(), digestmod='sha256')
    master_key = h.hexdigest()[:32].encode()  # 32 bytes means AES256
    cipher = AES.new(master_key, AES.MODE_CBC, iv.encode())
    ct = base64.b64decode(ct)
    pt = unpad(cipher.decrypt(ct), nb)
    return pt.decode()


class EncryptedText(str):
    def __init__(self, plain_text):
        self.iv = generate_an_iv()
        self.s1 = safe_random_string(16)
        self.s2 = safe_random_string(16)
        self.cipher_text = encrypt(self.s1, self.s2, self.iv, plain_text)

    def get(self):
        return decrypt(self.s1, self.s2, self.iv, self.cipher_text)

    def __repr__(self):
        return self.get()

    def __eq__(self, other):
        if isinstance(other, str):
            return self.get() == other
        elif isinstance(other, EncryptedText):
            return self.get() == other.get()
        else:
            return False

    def __hash__(self):
        return hash(self.cipher_text)


def is_private_key_encrypted(key_file):
    try:
        with open(key_file) as fp:
            private_key = fp.read()
        key = RSA.importKey(private_key.encode())
        if key.has_private():
            # If private key is not None, try to get its d parameter.
            return key.d is None  # d parameter is None meaning private key not decrypted.
        return True  # not a private key
    except ValueError:
        return True  # private key is encrypted
    except Exception:
        return None  # other exception


class CertVerifier(object):

    def __init__(self, file_name, text, is_ca=False) -> None:
        """
        init cert checker
        """
        self.file_name = file_name
        self.text = text
        self.is_ca = is_ca

    def verify_basic_constraints(self):
        if self.is_ca:
            ans = re.findall('Basic Constraints:[\s\S]*CA:TRUE',
                             self.text,
                             flags=re.IGNORECASE)
            return bool(ans)
        else:
            return True

    def verify_algorithm(self):
        """
        Get openssl output and verify the public key algorithm and key length.
        """
        ans = re.findall(
            r'Public Key Algorithm:[ ]*(.+)[\s\S]*Public\-Key:[ ]\([ ]*(\d+)[ ]*bit[ ]*\)',
            self.text,
            flags=re.IGNORECASE
        )
        if not (ans and len(ans[-1]) == 2):
            raise ValueError('Failed to get valid detail of public key algorithm.')

        algor, length = ans[-1]
        algor = algor.lower().strip()
        length = int(length)

        if algor not in ENCRYPTION_ALGORITHM_DETAILS:
            raise ValueError(f"Unsupported algorithm: '{algor}'. "
                             f"Please use one of the following: ECIES, RSA, or DSA.")

        details = ENCRYPTION_ALGORITHM_DETAILS[algor]
        if length < details['min_length']:
            raise ValueError(
                f'The encrypted length of the {algor.upper()} certificate {self.file_name} '
                f'must be greater than or equal {details["min_length"]} bits.'
            )
        if length < details['recommended_length']:
            utils.cli.write_to_terminal(
                f"WARNING: {self.file_name}: Current {'CA file' if self.is_ca else 'CERT file'} public key "
                f"length is {length} bits. {details['recommended_length']} bits are recommended.",
                color="yellow"
            )

        return True

    def verify_digest(self):
        """
        hash checker
        """
        ans = re.findall(r'Signature Algorithm:[ ]*([^\n ]*)[ ]*',
                         self.text,
                         flags=re.IGNORECASE)
        if not ans:
            return False
        hash_rule = {
            'sha0': r'(?!\d)',
            'md2': r'(?!\d)',
            'md4': r'(?!\d)',
            'md5': r'(?!\d)',
            'sha1': r'(?!\d)',
            'ripemd': r''
        }
        for key, value in hash_rule.items():
            illegal_ = re.findall(rf'{key}{value}',
                                  ans[-1],
                                  flags=re.IGNORECASE)
            if illegal_:
                raise ValueError(f'Failed to verify digest of {self.file_name}')
        return True


class CertCheckerHandler:

    @staticmethod
    def openssl_out(cert_path):
        """
        openssl output
        """
        process = subprocess.Popen(
            ['openssl', 'x509', '-noout', '-text', '-in', cert_path],
            shell=False,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        try:
            output, errors = process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            raise CertCheckException("Certificate file verification timed out.")
        except Exception:
            raise CertCheckException("Failed to verify certification file.")

        status = process.returncode
        output = output.decode('utf-8')[:-1]

        if status != 0:
            raise CertCheckException(output.strip())
        return output

    @staticmethod
    def is_valid_time_cert(ca_name):
        """
        cert time verfier
        """
        cert_path = os.path.realpath(ca_name)
        process = subprocess.Popen(
            ['openssl', 'x509', '--in', cert_path, '-checkend', '1'],
            shell=False,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        try:
            output, errors = process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            raise CertCheckException("Certificate time verification timed out.")
        except Exception:
            raise CertCheckException("Failed to verify certification time.")

        status = process.returncode
        output = output.decode('utf-8')[:-1]

        if status == 0 and output.lower().find('not expire') > -1:
            return True
        else:
            raise ValueError(f'Failed to check cert time: {cert_path}.')

    @staticmethod
    def verify_cert_format(cert_name, is_ca=False):
        """
        cert fomart verfier
        """
        result = CertCheckerHandler.openssl_out(os.path.realpath(cert_name))
        cv = CertVerifier(cert_name, result, is_ca=is_ca)
        if (cv.verify_algorithm()
                and cv.verify_digest()
                and CertCheckerHandler.is_valid_time_cert(cert_name)):
            return True
        return False

    @staticmethod
    def is_valid_cert(ca_name=None, crt_name=None):
        """
        cert checker sub function
        """
        if ca_name and not CertCheckerHandler.verify_cert_format(ca_name, is_ca=True):
            return False
        if crt_name and not CertCheckerHandler.verify_cert_format(crt_name, is_ca=False):
            return False
        return True
