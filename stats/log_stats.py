#!/usr/bin/env python3

import os
import time

stj_stats = "/Users/tiago/Dropbox/Inspira/STJ.txt"
tjsp_stats = "/Users/tiago/Dropbox/Inspira/TJSP.txt"

stj_cmd = "./stj.sh >> %s" %(stj_stats)
tjsp_cmd = "./tjsp.sh >> %s" %(tjsp_stats)

os.system(stj_cmd)
os.system(tjsp_cmd)
#	time.sleep(60 * 60)

