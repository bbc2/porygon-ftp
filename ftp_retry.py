import ftplib
import socket
from retry import with_max_retries

MAX_RETRIES = 10
EXCEPTIONS_TO_CATCH = (socket.timeout,)

class FTP_Retry(ftplib.FTP):
    pass

for name in ['nlst', 'size', 'pwd', 'cwd', 'sendcmd']:
    method = getattr(FTP_Retry, name)
    setattr(FTP_Retry, name, with_max_retries(MAX_RETRIES, EXCEPTIONS_TO_CATCH)(method))
