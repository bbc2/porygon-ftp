# Scanner timeout for FTP connections (seconds)
FTP_SCAN_TIMEOUT = 5

# Maximum number of concurrent connections. Should be lower than `ulimit -n`
FTP_SCAN_FILE_LIMIT = 900

# Indexer timeout for FTP connections (seconds)
FTP_INDEX_TIMEOUT = 30

# Maximum number of hits to return in a search
HIT_LIMIT = 200

from local_settings import *
