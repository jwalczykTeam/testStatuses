'''
Created on Jan 8, 2017

@author: alberto
'''

from __future__ import print_function
from doi.util import log_util
import elasticsearch
import logging
import sys
import pprint
import operator


logger = log_util.get_logger("astro")
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

def get_source_target_counts_map(aggs, rel_name):
    print("total number of aggs: {}".format(len(aggs)))

    source_target_counts_map = {}
    for agg in aggs:
        source = agg["key"]
        target_counts = {}
        target_count_buckets = agg[rel_name]["buckets"]
        for target_count_bucket in target_count_buckets:
            target = target_count_bucket["key"]
            count = target_count_bucket["doc_count"]
            target_counts[target] = count
        source_target_counts_map[source] = target_counts
            
    return source_target_counts_map

def get_target_total_counts(source_target_counts_map):
    target_total_counts = {}
    
    for target_counts in source_target_counts_map.values():
        for target, count in target_counts.items():
            total_count = target_total_counts.get(target)
            if total_count is None:
                total_count = 0
            total_count += count
            target_total_counts[target] = total_count
    
    return target_total_counts

def get_top_n_target_counts(source_target_counts_map, n=None):
    target_counts = get_target_total_counts(source_target_counts_map)
    sorted_target_counts = sorted(target_counts.items(), key=operator.itemgetter(1), reverse=True)
    if n is None:
        return sorted_target_counts
    
    if n > len(sorted_target_counts):
        n = len(sorted_target_counts)
    return sorted_target_counts[: n]

def get_inter_target_total_counts(source_target_counts_map, targets, aliases):
    for alias in aliases.keys():
        if alias in targets:        
            targets.remove(alias)
        
    inter_target_total_counts = {}
    for target in targets:
        inter_target_total_counts[target] = {}
        
    for target_counts in source_target_counts_map.values():
        for target_a in target_counts.keys():
            if target_a in aliases:
                target_a = aliases[target_a]
                
            if not target_a in targets:
                continue
            
            target_a_target_total_counts = inter_target_total_counts[target_a]
            for target_b, count in target_counts.items():
                if target_b in aliases:
                    target_b = aliases[target_b]

                if not target_b in targets:
                    continue
                
                target_a_target_b_count = target_a_target_total_counts.get(target_b)
                if target_a_target_b_count is None:  
                    target_a_target_b_count = 0
                target_a_target_b_count += count
                target_a_target_total_counts[target_b] = target_a_target_b_count

    return inter_target_total_counts

def print_source_target_counts_map(source_target_counts_map):
    print("total number of resource_authors entries: {}".format(len(source_target_counts_map)))
    for source, source_target_counts in source_target_counts_map.items():
        print(source)
        for target, count in source_target_counts.items():
            print("  {}={}".format(target.encode("utf8"), count))

def print_matrix_map(matrix_map, labels):
    print("labels ", end="")
    for label in labels:
        print(label.replace(' ', '_'), end=' ')
    
    print()
    for row_name in labels:
        row = matrix_map[row_name]
        print(row_name.replace(' ', '_'), end='\t')
        for col_name in labels:
            if col_name in row:
                value = row[col_name]
            else:
                value = 0

            print(value, end='\t')
        print()
    
def print_dict(dict):
    for key, value in dict.items():
            print("  {}={}".format(key.encode("utf8"), value))

def print_list(list):
    for item in list:
            print(item)

def do_the_job(es_url, index_name):
    client = elasticsearch.Elasticsearch([es_url])
    client.ping()

    query = {
        "size": 0,
        "query": {
            "query_string": {
                "query": "+_type: committed_file"
            }
        },
        "aggs": {
            "files": {
                "terms": {
                    "field": "file_path.raw",
                    "size": 1000000
                },
                "aggs": {
                    "authors": {
                        "terms": {
                            "field": "commit_author_name.raw",
                            "size": 1000000
                        }
                    }
                }                      
            }
        }
    }

    results = client.search(index=index_name, body=query)
    print("Total number of committed files analyzed............: {}".format(results['hits']['total']))
    #pp = pprint.PrettyPrinter(indent=2)
    #pp.pprint(results)

    files_aggs = results['aggregations']['files']['buckets']
    file_author_count_map = get_source_target_counts_map(files_aggs, "authors")
    print_source_target_counts_map(file_author_count_map)
    
    top_n_author_counts = get_top_n_target_counts(file_author_count_map, 10)
    print_list(top_n_author_counts)
    
    authors = set(zip(*top_n_author_counts)[0])
    inter_authors_total_counts = get_inter_target_total_counts(file_author_count_map, authors, 
                                                               {"alberto": "Alberto Giammaria"})
    print_source_target_counts_map(inter_authors_total_counts)
    print_matrix_map(inter_authors_total_counts, authors)
    
do_the_job("http://localhost:9200", "astro@alberto")
