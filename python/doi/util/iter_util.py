'''
Created on Dec 31, 2015

@author: alberto
'''


class Lookahead:
    def __init__(self, iterator):
        self.iterator = iterator
        self.ahead = None

    def __iter__(self):
        return self

    def next(self):
        if self.ahead is not None:
            ahead = self.ahead
            self.ahead = None
            return ahead
        else:
            return self.iterator.next()

    def lookahead(self):
        """Return an item ahead in the iteration."""
        if self.ahead is None:
            try:
                self.ahead = self.iterator.next()
            except StopIteration:
                return None
        return self.ahead
