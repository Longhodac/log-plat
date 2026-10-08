#!/usr/bin/env python3
"""Write a file of ERROR lines in the Loghub HDFS format, for an alerting demo.
Usage: gen_spike.py <out-file> <lines>
"""
import random
import sys

out, n = sys.argv[1], int(sys.argv[2])
random.seed(11)
with open(out, "w") as f:
    for i in range(n):
        blk = random.randint(10**17, 10**18)
        f.write(f"081109 20{35 + i // 3600:02d}{i // 60 % 60:02d} {100 + i % 90} ERROR dfs.DataNode$DataXceiver: "
                f"writeBlock blk_{blk} received exception java.io.IOException: Connection reset by peer\n")
