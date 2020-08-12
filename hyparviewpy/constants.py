"""Protocol constants."""

# join request codes
JOIN_REQ = 'jr'
JOIN_SUC = 'js'
JOIN_FOR = 'jf'

# neighbour request codes
NB_LOW = 'nl'
NB_HIG = 'nh'
NB_ACC = 'na'
NB_REJ = 'nr'

# shuffle codes
SHU_MES = 'sm'
SHU_REP = 'sr'

# message codes
B_MSG = 'bm'
D_MSG = 'dm'

# ping, disconnect and exit codes
PING = 'pi'
DISC = 'dc'
EXIT = 'ex'

# error codes
DATA_ERR = 'xd'
INVAL_ERR = 'xi'
MISC_ERR = 'xm'
TIMEOUT_ERR = 'xt'

# connection and message timeouts
CONN_TIMEOUT = 30
MSG_TIMEOUT = 30

# default fanout, active 'c', and passive 'k' constants
FAN_DEF = 4
ACT_C = 1
PAS_K = 5

# maximum number of broadcast messages to track
MAX_BCAST = 100

# active, passive and shuffle walk lengths
ACT_WALK = 6
PAS_WALK = 3
SHU_WALK = 6

# active and passive shuffle amounts
ACT_SHU = 3
PAS_SHU = 4

# time between shuffle rounds
SHU_TIME = 30

# sleep time between server loop iterations
SERV_SLEEP = 2
