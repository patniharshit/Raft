from datacenter import *
import sys
import time

id_ = int(sys.argv[1])
s1 = Server(id_)
s1.run()
