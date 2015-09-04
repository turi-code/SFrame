'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from datetime import tzinfo
from datetime import timedelta
class GMT(tzinfo):
    def __init__(self,ofs=None):
        if(ofs is None):
          self.offset = 0;
        else:
          self.offset = ofs
    def utcoffset(self, dt):
        return timedelta(minutes=self.offset * 60)
    def dst(self, dt):
        return timedelta(seconds=0)
    def tzname(self,dt):
        if(self.offset >= 0):
            return "GMT +"+str(self.offset)
        elif(self.offset < 0):
            return "GMT "+str(self.offset)
    def __str__(self):
        return self.tzname(self.offset)
    def  __repr__(self):
        return self.tzname(self.offset)
