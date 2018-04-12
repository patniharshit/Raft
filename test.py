from datacenter import *
import sys
import time

#id_ = int(sys.argv[1])
s1 = Server(1)
s2 = Server(2)
s3 = Server(3)
s4 = Server(4)
s5 = Server(5)
s1.run()
s2.run()
s3.run()
s4.run()
s5.run()
