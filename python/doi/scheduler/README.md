# Astro Jobs

An astro job consists of one or more tasks.
 
A task has an id and a python function that is executed on a worker.

Tasks can optionally depend on each other.

The following is an example of a mining job with two tasks: *mine_current_data* and *process_kibana_dashboards*. Both tasks take in input a git repo data source. 

The task *process_kibana_dashboards* will be executed only if the task *mine_current_data* is successful.

```
{
  "name": "vue", 
  "input": {
    "sources": [
      {
        "api_base_url": "https://api.github.com", 
        "git_url": "https://github.com/vuejs/vue.git", 
        "repo_name": "vue", 
        "repo_owner": "vuejs", 
        "source_type": "github_repo"
      }
    ]
  }, 
  "tasks": [
    {
      "id": "mine_current_data", 
      "function": "doi.jobs.miner.miner.mine_current_data"
    },
    {
      "id": "process_kibana_dashboards"
      "function": "doi.jobs.kibana.kibana.process_project_dashboards", 
      "depends_on": {
        "task_ids": [
          "mine_current_data"
        ]
      }
    } 
  ]
}
```


## Task Function

A task function has the following signature:

```
    task_f(job_id, job_name, task_id, method, input, previous_input, progress_callback, stop_event)
```

- **job_id** is a string representing the unique id of the job
- **job_name** is a string representing the full name of the job
- **task_id** is a string representing the unique id of the task 
- **method** can be one of the following values: "create", "update" or "delete". When a job is posted, "method" is "create" and the responsibility of the task is to create data.
When the job is updated, *method* is "update" and the responsibility of the task is to update the previously created data. 
When the job is deleted, *method* is "delete" and the responsibility of the task is to delete the previously created or updated data.
- **input** is a python dict representing the input of the task
- **previous_input** is used only when *method* is "update". It is a python dict representing the input of the task before the job update. This can be useful to find differences between new and previous input.
- **progress_callback** is a callback used for sending progress messages to the scheduler. The last message is stored in the database and is shown in the task objects returned by the REST API.
- **stop_event** is a python event used to communicate to the function that a stop was requested. The responsibility of the function is to stop execution as soon as possible and raise a TaskStoppedException indicating the stop request was executed successfully.


## Task Exceptions

A task function not raising any exceptions is considered successful.

A task function uses one of the following exceptions to indicate a failure.

```
class StoppedException(AstroException):
    def __init__(self):
		super(StoppedException, self).__init__("stopped")


class FailedException(AstroException):
    def __init__(self, type_, title, status, detail, **kwargs):
        super(FailedException, self).__init__(detail)
        self.type = type_
        self.title = title
        self.status = status
        self.kwargs = kwargs


class PermanentlyFailedException(FailedException):
    def __init__(self, type_, title, status, detail, **kwargs):
        super(FailedException, self).__init__(type_, title, status, detail, **kwargs)
```


A task raises the StoppedException exception to indicate its execution is stopped. Example:

```
	...
	if stop_event.is_set():
		raise StoppedException()

```

A task raises a FailedException to indicate a temporary failure. The task will be retried later. Example:


```
	...
	if not_enough_resources:
		raise FailedException(
			"about:blank", "Conflict", 409, 
			"Not enough resources to complete task", 
			cpu=cpu_value, mem=mem_value)

```

A task raises a PermanentlyFailedException to indicate it permanent failure. The task will not be retried. Example:

```
	...
	if not_found:
		raise PermanentlyFailedException(
			"about:blank", "Not found", 404, 
			"Repository {} not found".format(repo.name), 
			repo_url=repo.url)

```

Any other exception thrown by the task function is wrapped in a FailedException with a 500 *status_code*.

 
## Task lifecycle

A task function is initially executed with the "create" method. Later on it could be executed again with the "update" or the "delete" method in response to a job update or delete request.

In case the update or delete is requested while the task function is still running, the stop event (see last param of the task function) is set. The function that periodically checks this event stops its execution and raises a StoppedException. 

While this is the preferred approach, it is OK for a non-stoppable function to complete its execution.

Once the function execution is completed, it is called again with the "update" or "delete" method.
