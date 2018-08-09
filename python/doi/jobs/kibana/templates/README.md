# Visualizations and Dashboards  

Visualizations and dashboards are common to all projects we analyze with dev-risk-analysis plugin.
The templates of those visualizations and dashboards are contained in the files `visualizations.json` and
`dashboards.json`.  

## How to add a new visualization  

If you want to add a new visualization to be taken by the build process do the following:  

1. Create the visualization in the Kibana ui for a specific index (e.g. spark) and test it  

2. Export the visualization from Kibana ui: Settings -> Objects -> find your own and export  

3. Edit the exported file and change all the occurrences of the name of the index (e.g. spark) with the
   string `__index__`. For example  
   ```
   [
    {
      "_id": "spark-file-issues-predictions",
      "_type": "visualization",
      "_source": {
        "title": "spark file issues predictions",
        "visState": "{\"title\":\"spark issues probability\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMeticsAtAllLevels\":false},\"aggs\":[{\"id\":\"1\",\"type\":\"avg\",\"schema\":\"metric\",\"params\":{\"field\":\"probability\",\"customLabel\":\"Probability of getting an issue\"}},{\"id\":\"2\",\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"file_name.raw\",\"size\":500,\"order\":\"desc\",\"orderBy\":\"1\",\"customLabel\":\"Filename\"}}],\"listeners\":{}}",
        "uiStateJSON": "{}",
        "description": "",
        "kibanaSavedObjectMeta": {
          "searchSourceJSON": "{\"index\":\"spark\",\"query\":{\"query_string\":{\"query\":\"_type: file_issue_prediction\",\"analyze_wildcard\":true}},\"filter\":[]}"
        }
      }
    }
   ]
   ```
   Change all occurrences of `spark` with the placeholder `__index__`. To make the visualization nicer, you can use the placeholder `__title__` in the value of `_source.title` field, as in the example below.  
   
   ```
   [
    {
      "_id": "__index__-file-issues-predictions",
      "_type": "visualization",
      "_source": {
        "title": "__title__ File Issues Predictions",
        "visState": "{\"title\":\"__index__ issues probability\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMeticsAtAllLevels\":false},\"aggs\":[{\"id\":\"1\",\"type\":\"avg\",\"schema\":\"metric\",\"params\":{\"field\":\"probability\",\"customLabel\":\"Probability of getting an issue\"}},{\"id\":\"2\",\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"file_name.raw\",\"size\":500,\"order\":\"desc\",\"orderBy\":\"1\",\"customLabel\":\"Filename\"}}],\"listeners\":{}}",
        "uiStateJSON": "{}",
        "description": "",
        "kibanaSavedObjectMeta": {
          "searchSourceJSON": "{\"index\":\"__index__\",\"query\":{\"query_string\":{\"query\":\"_type: file_issue_prediction\",\"analyze_wildcard\":true}},\"filter\":[]}"
        }
      }
    }
   ]
   ```

4. Finally edit the file `visualizations.json` and append your new visualization.  


## Naming convention  

To achieve consistency among `astro` defined objects, it is suggested to follow a naming convention for all the `visualizations` and `dashboards` defined:  

- **_id** - Use lower-case words containing alphanumeric characters separated by `-` for object id
- **__title__** - Use `__title__` placeholder where the index name is going to be displayed in the ui:
  i.e. in `title` fields of Kibana objects.  

An example from the above snippet:  

- `"_id": "__index__-file-issues-predictions"`  
- `"title": "__title__ File Issues Predictions"`  


## How to modify a dashboard to include a new visualization  

If you want to include your new visualization in an existing dashboard do the following:  

1. Modify the dashboard in the Kibana ui for an existing project (e.g. spark)  

2. Export the dashboard from Kibana ui: Settings -> Objects -> find your own and export  

3. Edit the exported dashboard replacing the occurrences of spark with `__index__`.  
   Change `__index__` in `_source.title` value string to `__title__` to make the visualization title nicer.  

4. Finally, edit the `dashboards.json` file and replace the json object representing the dashboard
   that you want to modify, with the exported one.  

## How to create a new dashboard  

Follow the same steps for creating a new visualization and add the new object to the file
`dashboards.json`.  
