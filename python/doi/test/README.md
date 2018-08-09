# Get *top x* resources of a given *type* grouped by a given *field* in a *period of time*  

For all the examples below post the `jsons` to `<elasticsearch:port>/<index-name>/_search`  


Example:  
- top x: 20  
- type: issue  
- field: issue_type  
- period of time: "2014-07-01" TO "2014-07-29"  


```
{
	"size": 0,
	"query": {
		"query_string": {
			"query": "+_type: issue +timestamp:[\"2014-07-01\" TO \"2014-07-29\"]"
		}
	},
	"aggs": {
		"issue_types": {
			"terms": {
				"field": "issue_types.raw",
				"size": 20
			}
		}
	}
}
```

which results in something like:  

```
{
	"took": 5,
	"timed_out": false,
	"_shards": {
		"total": 5,
		"successful": 5,
		"failed": 0
	},
	"hits": {
		"total": 50,
		"max_score": 0,
		"hits": []
	},
	"aggregations": {
		"issue_types": {
			"doc_count_error_upper_bound": 0,
			"sum_other_doc_count": 0,
			"buckets": [{
				"key": "Bug",
				"doc_count": 21
			},
			{
				"key": "Improvement",
				"doc_count": 13
			},
			{
				"key": "Sub-task",
				"doc_count": 9
			},
			{
				"key": "Test",
				"doc_count": 2
			},
			{
				"key": "New Feature",
				"doc_count": 1
			},
			{
				"key": "Wish",
				"doc_count": 1
			}]
		}
	}
}
```


# Get an *interval* time series of *top x* resources of a given *type* grouped by a given *field* in a *period of time*  

Example:  
- top x: 20  
- type: issue  
- field: issue_type  
- period of time: "2014-07-01" TO "2014-07-29"  
- interval: day (daily aggregation)  


```
{
	"size": 0,
	"query": {
		"query_string": {
			"query": "+_type: issue +timestamp:[\"2014-07-01\" TO \"2014-07-31\"]"
		}
	},
	"aggs": {
		"time_series": {
			"date_histogram": {
				"field": "timestamp",
				"interval": "week",
				"missing": "2000-01-01"
			},
			"aggs": {
				"issue_types": {
					"terms": {
						"field": "issue_types.raw",
						"size": 20
					}
				}
			}
		}
	}
}
```

which results in something like:  

```
{
	"took": 4,
	"timed_out": false,
	"_shards": {
		"total": 5,
		"successful": 5,
		"failed": 0
	},
	"hits": {
		"total": 52,
		"max_score": 0,
		"hits": []
	},
	"aggregations": {
		"time_series": {
			"buckets": [{
				"key_as_string": "2014-06-30T00:00:00.000Z",
				"key": 1404086400000,
				"doc_count": 8,
				"issue_types": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 0,
					"buckets": [{
						"key": "Bug",
						"doc_count": 3
					},
					{
						"key": "Improvement",
						"doc_count": 2
					}]
				}
			},
			{
				"key_as_string": "2014-07-07T00:00:00.000Z",
				"key": 1404691200000,
				"doc_count": 9,
				"issue_types": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 0,
					"buckets": [{
						"key": "Improvement",
						"doc_count": 4
					},
					{
						"key": "Bug",
						"doc_count": 3
					},
					{
						"key": "New Feature",
						"doc_count": 1
					},
					{
						"key": "Test",
						"doc_count": 1
					}]
				}
			},
			{
				"key_as_string": "2014-07-14T00:00:00.000Z",
				"key": 1405296000000,
				"doc_count": 15,
				"issue_types": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 0,
					"buckets": [{
						"key": "Bug",
						"doc_count": 6
					},
					{
						"key": "Improvement",
						"doc_count": 4
					},
					{
						"key": "Sub-task",
						"doc_count": 4
					},
					{
						"key": "Wish",
						"doc_count": 1
					}]
				}
			},
			{
				"key_as_string": "2014-07-21T00:00:00.000Z",
				"key": 1405900800000,
				"doc_count": 12,
				"issue_types": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 0,
					"buckets": [{
						"key": "Bug",
						"doc_count": 6
					},
					{
						"key": "Sub-task",
						"doc_count": 3
					},
					{
						"key": "Improvement",
						"doc_count": 2
					},
					{
						"key": "Test",
						"doc_count": 1
					}]
				}
			},
			{
				"key_as_string": "2014-07-28T00:00:00.000Z",
				"key": 1406505600000,
				"doc_count": 8,
				"issue_types": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 0,
					"buckets": [{
						"key": "Bug",
						"doc_count": 5
					},
					{
						"key": "Sub-task",
						"doc_count": 2
					},
					{
						"key": "Improvement",
						"doc_count": 1
					}]
				}
			}]
		}
	}
}
```
