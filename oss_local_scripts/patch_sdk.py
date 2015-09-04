#!/usr/bin/python
import re
import sys
import os
print "patching ", sys.argv[1]
includestring = re.compile("#include\s?<(.*hp*)>")

def remap_line(i):
    t = includestring.match(i)
    if t:
        g = t.group(1)
        print g
        if os.path.isfile('graphlab/' + g):
            return "#include <graphlab/" + g + ">\n"
    return i

lines = file(sys.argv[1], 'r').readlines()
lines = [remap_line(l) for l in lines]
nf = file(sys.argv[1], 'w')
nf.writelines(lines)
nf.close()
print
