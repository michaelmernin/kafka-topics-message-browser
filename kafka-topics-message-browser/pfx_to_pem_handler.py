import contextlib
import tempfile

import OpenSSL.crypto

from config_handler import ConnectionConfig


class PFXReader:

    @contextlib.contextmanager
    def pfx_to_pem(self, environment):
        """
        Converts pfx to pem, as some kafka-registry's do not accept pem files
        :param environment: string of request topic
        :return: pem formatted file
        """
        config = ConnectionConfig.connection_details
        ''' Decrypts the .pfx file to be used with requests. '''
        with tempfile.NamedTemporaryFile(suffix='.pem', delete=False) as t_pem:
            f_pem = open(t_pem.name, 'wb')
            pfx = open(config['ssl'][environment]['pfx_file'], 'rb').read()
            p12 = OpenSSL.crypto.load_pkcs12(pfx, config['ssl'][environment]['ssl.key.password'])
            f_pem.write(OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey()))
            f_pem.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate()))
            ca = p12.get_ca_certificates()
            if ca is not None:
                for cert in ca:
                    f_pem.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, cert))
            f_pem.close()
            yield t_pem.name
