'''
Created on Dec 31, 2015

@author: alberto
'''

from doi.util import log_util
import errno
import fnmatch
import gzip
import operator
import os
import shutil


PROGRAMMING_FILE_EXTENSIONS = {"java", "scala", "py", "js", "r", "c", "cpp", "h", "m"}
BINARY_FILE_EXTENSIONS = {"jar", "lib", "dll"}

logger = log_util.get_logger("astro.util.file_util")


def is_programming_file(name):
    file_ext = get_file_ext(name)
    if file_ext is None:
        return False

    file_ext = file_ext.lower()
    return file_ext in PROGRAMMING_FILE_EXTENSIONS

def is_binary_file(name):
    file_ext = get_file_ext(name)
    if file_ext is None:
        return False
    
    file_ext = file_ext.lower()
    return file_ext in BINARY_FILE_EXTENSIONS

def get_file_ext(name):
    last_dot_index = name.rfind('.')
    if last_dot_index != -1:
        return name[last_dot_index +1:]
    else:
        return None

def get_file_name_no_ext(name):
    last_dot_index = name.rfind('.')
    if last_dot_index != -1:
        return name[: last_dot_index]
    else:
        return name

def compress_file(in_file_name, out_file_name):
    with open(in_file_name, 'rb') as in_file, gzip.open(out_file_name, 'wb') as out_file:
        shutil.copyfileobj(in_file, out_file)

def makedirs(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise  # Raises the error again

def get_dir_file_paths(dir_path, excluded_files=[]):
    def excluded(file_path, excluded_files):
        for excluded_file in excluded_files:
            if fnmatch.fnmatch(file_path, excluded_file):
                return True
        return False
    
    file_paths = [] 
    for parent_dir_path, _, file_names in os.walk(str(dir_path)):
        for file_name in file_names:
            file_path = os.path.join(parent_dir_path, file_name)
            rel_file_path = os.path.relpath(file_path, dir_path)
            if not excluded(rel_file_path, excluded_files):
                file_paths.append(rel_file_path)
    
    file_paths.sort()
    return file_paths 

def get_dir_file_exts_stats(dir_name):
    count_by_file_ext = {}    
    for _, _, file_names in os.walk(str(dir_name)):
        for file_name in file_names:
            file_ext =  get_file_ext(file_name)            
            ext_count = count_by_file_ext.get(file_ext)
            if ext_count is None:
                ext_count = 0
                
            count_by_file_ext[file_ext] = ext_count +1
                        
    return sorted(count_by_file_ext.items(), key=operator.itemgetter(1), reverse=True)                        

def file_matches(path, file_patterns=None):
    if file_patterns is None:
        return True
    
    for file_pattern in file_patterns:
        if fnmatch.fnmatch(path, file_pattern):
            return True
            
    return False

def list_files(dir_path):
    file_paths = []
    for entry_name in os.listdir(str(dir_path)):
        entry_path = os.path.join(dir_path, entry_name)
        if os.path.isfile(entry_path):
            file_paths.append(entry_path)
            
    return file_paths 

def list_dirs_at_level(dir_path, level):
    def add_dirs(dir_path, current_level, nested_dir_paths):        
        for entry_name in os.listdir(str(dir_path)): 
            entry_path = os.path.join(dir_path, entry_name)
            if os.path.isdir(entry_path):
                if current_level == level:
                    nested_dir_paths.append(entry_path)                    
                if current_level < level:
                    add_dirs(entry_path, current_level +1, nested_dir_paths)

    nested_dir_paths = []
    add_dirs(dir_path, 1, nested_dir_paths)
    return nested_dir_paths

def remove_dir_tree(path):
    if os.path.isdir(path):
        shutil.rmtree(str(path))

def is_path_absolute(path):
    return os.path.isabs(path)    


SHELL_SPECIAL_CHARS = " !\"$&'()*,:;<=>?@[\\]^`{|}"

def escape_git_diff_path(path):
    # Clean path of any trailing TABs
    cleaned_path = path.rstrip("\t")
    
    # Unescape path for any \<chr> characters
    unescaped_path = cleaned_path
    if cleaned_path.startswith('"') and cleaned_path.endswith('"'):
        unescaped_path = cleaned_path[1:-1].decode('string-escape')
        
    # Escape path for shell special characters
    escaped_path = ""
    for c in unescaped_path:
        if c in SHELL_SPECIAL_CHARS:
            escaped_path += "\\"
        escaped_path += c

    if path != escaped_path:
        logger.debug("Special characters found in git diff path: '%s' -> '%s'", path, escaped_path)

    return escaped_path
