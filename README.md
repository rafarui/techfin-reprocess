#### Funcitons already implemented.

In functions.carol_task.py

1. `cancel_tasks`: Cancell tasks given task list. 
2. `track_tasks`: track tasks in carol
3. `drop_staging`: drop a list of staging tables
4. `get_all_stagings`: get all staging tables from a connector
5. `get_all_etls`: get all ETLs from a connector
6. `drop_single_etl`: drop ETL from a staging table using the output tables.
7. `drop_etls`: Drop ETLs from ETL list.
8. `par_processing`: process a list of staging tables in parallel. 
9. `pause_and_clear_subscriptions`: Pause and clear datamodel subscription
10. `play_subscriptions`: play a data model subscription
11. `find_task_types`: find all the running/pending process and reprocess tasks
12. `pause_etls`: Pause a list of ETLs.
13. `pause_single_staging_etl`: Pause a single ETL from a staging based on the output tables.
14.  `pause_dms`: Pause mapping from a list of data model names.
15.  `par_consolidate`: consolidate multiple staging tables in parallel
16.  `consolidate_stagings` consolidate multiple staging tables
17.  `par_delete_golden`: delete all golden/rejected records from a list of datamodel in parallel
18.  `par_delete_staging`: delete all staging records from a list of staging tables in parallel
19.  `resume_process`: resume process from a staging table.
20.  `check_mapping`: check if staging table has a mapping.
21.  `check_lookup`: check if a staing table is lookup table.
22.  `change_app_settings`: change app settings.
23.  `start_app_process`: start app process.
24.  `get_relationships`: get relationships from a data model
25.  `get_relationship_constraints`: get relationship constraints from a data model
26.  `remove_relationships`: remove relationships from a data model
27.  `remove_relationships_and_delete_data_model`: remove relationship and delete a data model
28.  `remove_dms`: remove relationship and delete data models from a list
29. `enable_disable_storage_type`: enable or disable a Carol storage type.4
30. `disable_all_rt_storage`: Disable all the RT storage in a tenant.
31. `enable_data_decoration`: enable data decoration for a tenant. 


In functions.carol_apps.py

1. `get_app_version`: Get app version
2. `check_failed_instalL`: Check if there is a faild app task instalation for the current version,
3. `update_app` update app version.