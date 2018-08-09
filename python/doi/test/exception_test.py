'''
Created on Dec 18, 2016

@author: alberto
'''

class O(object):
    def __init__(self):
        self.a = None
        self.b = None
        self.c = None
        
        self.a = 1
        self.b = 2
        self.c = 3
        
    def __del__(self):
        print("__del__ called")
        self.close()
        
    def close(self):
        print("__close__ called")
        if self.a is not None:
            print(self.a)
        
        if self.b is not None:
            print(self.b)

        if self.c is not None:
            print(self.c)

try:
    o = None
    o = O()
    o.a = 11
    d = 1/0
    o.b = 12
    o.c = 13
    
    global n
    if n < 35:
        n += 1
        a = 1/0
    else:
        n = 0
    
finally:
    print("finally block entered")
    if o is not None:
        o.close()
    
