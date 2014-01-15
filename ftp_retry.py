import ftplib
import socket
from retry import with_max_retries

MAX_RETRIES = 10
EXCEPTIONS_TO_CATCH = (socket.timeout, EOFError)

class FTP_Retry(ftplib.FTP):
    @with_max_retries(MAX_RETRIES, EXCEPTIONS_TO_CATCH)
    def mlsd(self, *args, **kwargs):
        return [f for f in super(FTP_Retry, self).mlsd(*args, **kwargs)]

for name in ['pwd', 'cwd']:
    method = getattr(FTP_Retry, name)
    setattr(FTP_Retry, name, with_max_retries(MAX_RETRIES, EXCEPTIONS_TO_CATCH)(method))
