'''
Created on Dec 31, 2015

@author: alberto
'''


from doi.util import log_util
from doi.util import iter_util
import re
import StringIO


logger = log_util.get_logger("astro.util.diff_util")


class Change(object):
    '''
    classdocs
    '''

    def __init__(self, kind, line_number, line):
        '''
        Constructor
        '''
        self.kind = kind
        self.line_number = line_number
        self.line = line

                
class Hunk(object):
    '''
    classdocs
    '''

    def __init__(self, base_start, base_count, 
                 modified_start, modified_count,
                 changes):
        '''
        Constructor
        '''
        self.base_start = base_start
        self.base_count = base_count
        self.modified_start = modified_start
        self.modified_count = modified_count
        self.changes = changes
        
        

class Diff(object):
    '''
    classdocs
    '''

    def __init__(self, 
                 a_path, b_path,
                 copied_from, copied_to,
                 renamed_from, renamed_to,
                 index,
                 hunks, 
                 added_lines, deleted_lines, 
                 added_size, deleted_size):
        '''
        Constructor
        '''
        self.a_path = a_path
        self.b_path = b_path
        self.copied_from = copied_from
        self.copied_to = copied_to
        self.renamed_from = renamed_from
        self.renamed_to = renamed_to
        self.index = index
        self.hunks = hunks
        self.added_lines = added_lines        
        self.deleted_lines = deleted_lines        
        self.added_size = added_size        
        self.deleted_size = deleted_size
        
            
    def is_binary(self):
        return (self.a_path is None and self.b_path is None and
                self.copied_from is None and self.copied_to is None and
                self.renamed_from is None and self.renamed_to is None)
         
    def get_action(self):
        if (self.renamed_from is not None and 
            self.renamed_to is not None):
            return 'renamed'

        if (self.copied_from is not None and 
            self.copied_to is not None):
            return 'added'  # for github 'get commit' compatibility

        if self.a_path is None:
            return 'added'
        
        if self.b_path is None:
            return 'removed'
        
        return 'modified'


    def get_path(self):
        if self.b_path is not None:
            # When the file is added or modified
            return self.b_path

        if self.renamed_to is not None:
            # When the file is renamed            
            return self.renamed_to

        if self.copied_to is not None:
            # When the file is copied
            return self.copied_to
        
        if self.a_path is not None:
            # In case the file is removed
            return self.a_path
        
        assert self.a_path is not None or self.b_path is not None or self.renamed_to is not None or self.copied_to is not None, \
                "invalid diff state: a_path='{}', b_path={}, copied_from={}, copied_to={}, renamed_from={}, renamed_to={}".format(
                        self.a_path, self.b_path, self.copied_from, self.copied_to, self.renamed_from, self.renamed_to)
        

    def get_previous_path(self):
        if self.a_path is not None:
            return self.a_path

        if self.renamed_from is not None:
            return self.renamed_from

        if self.copied_from is not None:
            return self.copied_from
        
        assert self.a_path is not None or self.renamed_from is not None or self.copied_from is not None, \
                "invalid diff state: a_path='{}', b_path={}, copied_from={}, copied_to={}, renamed_from={}, renamed_to={}".format(
                        self.a_path, self.b_path, self.copied_from, self.copied_to, self.renamed_from, self.renamed_to)
        

def parse_patch(patch, parse_header=False):
    a_path = None
    b_path = None
    copied_from = None
    copied_to = None
    renamed_from = None
    renamed_to = None
    index = None
    
    if parse_header:
        buf = StringIO.StringIO(patch)
        line = buf.readline()
        while line and not line.startswith("@@ "):
            if line.startswith("diff --"):
                pass
            elif line.startswith("rename from "):
                renamed_from = line[12:].rstrip('\n')
            elif line.startswith("rename to "):
                renamed_to = line[10:].rstrip('\n')
            elif line.startswith("copy from "):
                copied_from = line[10:].rstrip('\n')
            elif line.startswith("copy to "):
                copied_to = line[8:].rstrip('\n')
            elif line.startswith("index "):
                index = line[6:].rstrip('\n')
            elif line.startswith("--- "):
                a_path = _get_ab_path(line)
            elif line.startswith("+++ "):
                b_path = _get_ab_path(line)
            line = buf.readline()
        
        
    #hunk_header = r'\@\@\s-(\d+),(\d+)\s\+(\d+),(\d+)\s\@\@(.*)$'
    hunk_header  = r'\@\@\s-(\d+)(?:,(\d+))?\s\+(\d+)(?:,(\d+))?\s\@\@(.*)$'
    hunk_matches = iter_util.Lookahead(re.finditer(hunk_header, patch, re.MULTILINE))
    
    hunks = []
    added_lines = 0
    deleted_lines = 0
    added_size = 0
    deleted_size = 0
    for hunk_match in hunk_matches:
        base_start = int(hunk_match.group(1))
        count = hunk_match.group(2)
        if count is not None:
            base_count = int(count)
        else:
            base_count = 0
        modified_start = int(hunk_match.group(3))
        count = hunk_match.group(4)
        if count is not None:
            modified_count = int(count)
        else:
            modified_count = 0
      
        next_hunk_match = hunk_matches.lookahead()
        if next_hunk_match is not None:
            rest = patch[hunk_match.end() +1:next_hunk_match.start()]          
        else:
            rest = patch[hunk_match.end() +1:]

        #lines = rest.splitlines() !!!! cannot use splitlines, node.js for example has \r in the middle of added (+) lines
        lines = rest.split('\n')
        if lines[-1] == "":
            lines = lines[: -1]
            
        changes = []
        base_number = base_start
        modified_number = modified_start
        for line in lines:
            if not line:
                logger.warning("warning: empty line found in git patch: "
                               "a_path='%s', b_path='%s'", a_path, b_path)
                continue

            kind = line[0]
            if kind == '+':
                # Added line
                changes.append(Change(kind, modified_number, line[1:]))
                modified_number += 1
                added_lines += 1
                added_size += len(line) # new line included
            elif kind == '-':
                # Removed line
                changes.append(Change(kind, base_number, line[1:]))
                base_number += 1
                deleted_lines += 1
                deleted_size += len(line) # new line included
            elif kind == ' ':
                # Context
                base_number += 1
                modified_number += 1
            else:
                if line != "\ No newline at end of file":
                    logger.warning("warning: extraneous line found in git patch: "
                                   "a_path='%s', b_path='%s', line='%s'", 
                                   a_path, b_path, line)
                
        hunks.append(Hunk(base_start, base_count, 
                          modified_start, modified_count,
                          changes))
    
    return Diff(a_path, b_path,
                copied_from, copied_to,
                renamed_from, renamed_to,
                index,
                hunks, 
                added_lines, deleted_lines, 
                added_size, deleted_size)
    
def _get_ab_path(line):
    # Remove leading "+++ " or "--- " and trailing '\t' and '\n'
    line = line[4:].rstrip('\t\n')
    
    if line == "/dev/null":
        return None
    
    if line.startswith('"') and line.endswith('"'):
        # Remove "/a or "/b and add leading "
        return '"' + line[3:]
    
    # Remove /a or /b
    return line[2:]
    
