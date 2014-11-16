# FTP port
PORT = 21

# Maximum duration from an initial probe to a successful login
SCAN_TIMEOUT = 20

# Maximum simultaneous scan tasks
MAX_SCAN_TASKS = 1000

# Interval between scans
SCAN_INTERVAL = 10 * 60

# Offline time after which a server is forgotten
OFFLINE_DELAY = 24 * 3600

# Timeout for the connection to an FTP server during indexation
INDEX_TIMEOUT = 30

# Maximum simultaneous index tasks
MAX_INDEX_TASKS = 2

# Minimum interval between index tasks on a given host
INDEX_INTERVAL = 4 * 3600

# Maximum number of FTP errors allowed during the indexation of a server
MAX_INDEX_ERRORS = 10

# Signals to catch
SOFT_SIGNALS = ['SIGINT', 'SIGTERM']
