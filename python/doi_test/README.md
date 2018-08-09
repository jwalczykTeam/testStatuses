# astro-test  

Test framework and test suites for Astro project.  


## Usage  

Simplest usage is:  

```
export PYTHONPATH=<your path to astro/python + your path to astro-test/python>
cd python/doi_test/test_suites
python -m unittest discover
```

which executes all the test suites stored on the `test_suites` folder.  
A test may require data, which are usually stored on the `test_suites/data` folder. Data
may reference variables (using the syntax `${variable-name}`), which are expected to
be set in the environment.  If you do not, then you should get assertion errors.  
Set the variables as you get those errors and re-run the tests.  

You can run selected test suites and test classes using the [Python unittest syntax](https://docs.python.org/2/library/unittest.html#command-line-interface).  
A few examples below:  

```
python -m unittest test_module1 test_module2
python -m unittest test_module.TestClass
python -m unittest test_module.TestClass.test_method
```


## The AstroInstance class  

The `AstroInstance` class defines many facilities to run complex tests, like setting up an
Astro `Scheduler` with one or more `Workers` and one or more `API servers`,
calling the `API`, and finally tearing down the environment.  
The `AstroInstance` is an abstract class. It has two implementations, one that runs locally,
on the current box, and one that runs remotely on `IBM Containers`.  
So, you'll write your test suites once, and you'll run them locally or on a real IBM Containers environment.  

- Running locally means that all the Astro components are launched in separate python processes on the
current box: one or more `worker_app.py`, one or more `api_app.py`, one `scheduler_app.py`.  

- Running remotely means that all the Astro components are launched on IBM Containers, given a
Bluemix account / org / space.  

Take a look at the test suites to learn how to use the `AstroInstance` class. In particular
the module `doi_test.test_suites.test_suite_sample.py` can be used to
support the reading of next paragraphs, and as examples to write your own test cases.  


## How to write a test suite  

Here are the few steps you should follow to write a test suite:  

1. Get an instance of the `AstroInstance` class calling the
   `doi_test.util.factory.get_astro_instance()` function.  
   The function:  
   ```
   def get_astro_instance(config, test_module=None, test_class=None)
   ```
   takes as parameters the path to a configuration file: `config`,
   which is the json file where you provide runtime parameters of
   the Astro instance you are deploying during the test (see below
   the complete description of its format)  

2. Write a class derived from `unittest.TestCase` providing
   the methods:  
   - **setUp()** for deploying the instance of Astro that you need
     in your test (locally or remotely, it is totally transparent
     to the activity of writing the test, it is decided at runtime
     setting an environment variable).  
     It is not required to stand up an instance of Astro, you can
     reference an existing one to run your test.  
   - **tearDown()** for removing the instance of Astro created in
     the `setUp()`.  
     It is not required to tear down an environment after the test,
     you can use the test to deploy an Astro, run a smoke test, and
     leave it running.  
   - **testYourTestHere()**, which is where you implement the test
     you want to run.  
     This method must start with `test`, so that the `pyton unittest`
     framework will recognize it as the test method to run.  
     You can write as many `test` methods as you need, but remember
     that the `python unittest` framework will run setUp() and
     tearDown() for each test.  
     The `AstroInstance.astro_api()` method returns a
     simplified interface to Astro API, so you can effectively write
     API calls, test status codes, wait on projects state changes
     while running, and so on. Look at the test methods in
     `doi_test.test_suites.test_suite_sample.py` to take
     example snippets of code.  


## How to run a test suite locally  

1. Write the test config json file (see below how to)  
2. Copy the file `python/doi_test/setenv.sh.template` to
   a `setenv.sh` and customize it as described in the template,
   in particular you have to decide whether you want to use local
   Elasticsearch and/or Kafka installations, or as a service on Bluemix  
3. Source the `setenv.sh` script  
4. Run your test as  
   ```
   python -m unittest doi_test.test_suites.test_suite_sample.DriverCrashes
   ```
   or
   ```
   python -m unittest doi_test.test_suites.test_suite_sample
   ```
   to run all tests in the module.  
5. Log files of the Astro components are stored on
   `/tmp/astro/<test_suite_name>/<test_class_name>`.  
   You can change the root `/tmp/astro` setting the `work_dir`
   property in the config json file of the test.  


## How to run a test suite remotely  

The steps are the same as running tests locally, you only need to
read different instructions in the `setenv.sh.template` file, so:  

1. Write the test config json file (see below how to)  
2. Copy the file `python/doi_test/setenv.sh.template` to
   a `setenv.sh` and customize it as described in the template  
3. Source the `setenv.sh` script  
4. Run your test as  
   ```
   python -m unittest doi_test.test_suites.test_suite_sample.DriverCrashes
   ```
   or
   ```
   python -m unittest doi_test.test_suites.test_suite_sample
   ```
   to run all tests in the module.  
5. Log files of the Astro components are collected from IBM Containers
   at the end of the test, and are stored on
   `/tmp/astro/<test_suite_name>/<test_class_name>`.  
   You can change the root `/tmp/astro` setting the `work_dir`
   property in the config json file of the test.  


### How to write a test config json file (configure an AstroInstance)  

Look at one of the json files on the `doi_test/test_suites/data`
folder.  

The file has two sections: `local` and `remote`. At least one should be
set depending on whether you want to run the tests locally or remotely, or
both.  

```
{
  "local": {
    "group_name": "mkrudele",
    "work_dir": "/tmp/astro",
    "api_key": "barabba",
    "tear_down": { "run": true },
    "mh_config": ${TEST_MH_CONFIG},
    "db_config": ${TEST_DB_CONFIG}
  },
  "remote": {
    "group_name": "${TEST_GROUP_NAME}",
    "scheduler": {
      "memory": 512,
      "additional_env": ["ASTRO_LAST_COMMIT_SHA=put-here-the-last-commit-sha", "NEW_RELIC_LICENSE=''"]
    },
    "worker_group": {
      "memory": 512,
      "additional_env": ["ASTRO_LAST_COMMIT_SHA=put-here-the-last-commit-SHA", "NEW_RELIC_LICENSE=''"],
      "out_dir": "/tmp/astro"
    },
    "api_group": {
      "domain": "mybluemix.net",
      "port": 8080,
      "memory": 512,
      "additional_env": ["ASTRO_LAST_COMMIT_SHA=put-here-the-last-commit-SHA", "NEW_RELIC_LICENSE=''"]
    },
    "work_dir": "/tmp/astro",
    "api_key": "barabba",
    "image_name": "${ASTRO_DOCKER_IMAGE_NAME}",
    "tear_down": { "run": true, "clear": true, "reset": true },
    "containerbridge_app": "containerbridge"
  },
  ...
}
```

1. You can specify variables that should be resolved at runtime using the
   `${env_var_name}` syntax. The `doi_test.util.common.load_data_file(jsonfile)`
   function will resolve them and substitute in the returned `python dict` that you should
   pass to the factory method that will build the `AstroInstance`
   (`doi_test.util.factory.get_astro_instance()`).
   You can set those variables in the `setenv.sh` script that you source before running
   the python unittest.  

2. You can add additional attributes to the file, which you can use for your own test. For example
   if you are mining a GitHub project you can add the payload to the `POST /v1/projects` as
   the value of `my_test_project`.  
   The `AstroInstance` will ignore any attribute other than `local` or `remote`.  

3. You can configure the Astro deployment using attributes in the `local` or `remote`. These
   are the recognized names in the `remote` section and meaning (**bold** ones are mandatory):  
   - **group_name**, the logical group name of Astro. It is used to build default names for Astro
     components (e.g. scheduler-<group_name>-<space_name>)  
   - *work_dir*, the folder where test log files are stored. Defaulted to `/tmp/astro`  
   - **api_key**, the API_KEY of the Astro instance deployed (or being used) in the tests  
   - *api_port*, the listening port of the Astro API servers. Default is `8080`  
   - **image_name**, the name of the Astro image to use when deploying a new Astro environment
     on IBM Containers  
   - *tear_down*, where you can configure the actions to perform on tearing down the environment.  
     - `tear_down.run` false to not tear down the deployed environment
       after running the test
     - `tear_down.clear` true to gracefully remove all the projects from
       deployed Astro
     - `tear_down.reset` true to cleanup Elasticsearch and Messagehub from
       artifacts created by the test  

    Default is run = true, clear = true, and reset = true  
   - *containerbridge_app*, the name of the Bluemix app to use for binding services to the
     IBM Containers. It defaults to `containerbridge`  
   - **scheduler** contains the configuration of the `Astro Scheduler`  
   - **worker_group** contains the configuration of the `Astro Worker` containers group  
   - **api_group** contains the configuration of the `Astro API Server` containers group  
