from pycarol import (
    Carol, ApiKeyAuth, PwdAuth, Tasks, Staging, Connectors, CDSStaging, Subscription, DataModel, Apps
)
from pycarol.exceptions import CarolApiResponseException
from pycarol.data_models import CreateDataModel
from pycarol import CDSGolden
from pycarol.query import delete_golden
from collections import defaultdict
import random
import time
import logging
from joblib import Parallel, delayed
from itertools import chain
from pycarol.exceptions import CarolApiResponseException
from functions import misc
import copy


def cancel_tasks(login, task_list, logger=None):
    """Cancell tasks from Carol

    Args:
        login (pycarol.Carol): Instance of pycarol.Carol
        task_list (list): list of carol tasks
        logger (logger, optional): logger to log informations. Defaults to None.
    """
    if logger is None:
        logger = logging.getLogger(login.domain)

    carol_task = Tasks(login)
    for i in task_list:
        logger.debug(f"Canceling {i}")
        carol_task.cancel(task_id=i, force=True)

    return


def track_tasks(login, task_list, retry_count=3, logger=None, callback=None, ):
    """Track a list of taks from carol, waiting for errors/completeness. 

    Args:
        login (pycarol.Carol): pycarol.Carol instance
        task_list (list): List of tasks in Carol
        retry_count (int, optional): Number of times to restart a failed task. Defaults to 3.
        logger (logger, optional): logger to log information. Defaults to None.
        callback (calable, optional): This function will be called every time task status are fetch from carol. Defaults to None.

    Returns:
        [dict]: dict with status of each task.
    """
    if logger is None:
        logger = logging.getLogger(login.domain)

    retry_tasks = defaultdict(int)
    n_task = len(task_list)
    max_retries = set()
    carol_task = Tasks(login)
    while True:
        task_status = defaultdict(list)
        for task in task_list:
            status = carol_task.get_task(task).task_status
            task_status[status].append(task)
        for task in task_status['FAILED'] + task_status['CANCELED']:
            logger.warning(f'Something went wrong while processing: {task}')
            retry_tasks[task] += 1
            if retry_tasks[task] > retry_count:
                max_retries.update([task])
                logger.error(
                    f'Task: {task} failed {retry_count} times. will not restart')
                continue

            logger.info(f'Retry task: {task}')
            login.call_api(path=f'v1/tasks/{task}/reprocess', method='POST')

        if len(task_status['COMPLETED']) == n_task:
            logger.debug(f'All task finished')
            return task_status, False

        elif len(max_retries) + len(task_status['COMPLETED']) == n_task:
            logger.warning(f'There are {len(max_retries)} failed tasks.')
            return task_status, True
        else:
            time.sleep(round(10 + random.random() * 5, 2))
            logger.debug('Waiting for tasks')
        if callable(callback):
            callback()


def drop_staging(login, staging_list, connector_name, logger=None):
    """
    Drop a list of stagings

    Args:
        login: pycarol.Carol
            Carol() instance.
        staging_list: list
            List of stagings to drop
        logger:
            Logger to be used. If None will use
                logger = logging.getLogger(login.domain)

    Returns: list, status
        List of tasks created, fail status.

    """

    if logger is None:
        logger = logging.getLogger(login.domain)

    tasks = []
    for i in staging_list:
        stag = Staging(login)

        try:
            r = stag.drop_staging(
                staging_name=i, connector_name=connector_name, )
            tasks += [r['taskId']]
            logger.debug(f"dropping {i} - {r['taskId']}")

        except CarolApiResponseException as e:
            if 'SCHEMA_NOT_FOUND' in str(e):
                logger.debug(f"{i} already dropped.")
                continue
            else:
                logger.error("error dropping staging", exc_info=1)
                return tasks, True

        except Exception as e:
            logger.error("error dropping staging", exc_info=1)
            return tasks, True

    return tasks, False


def get_all_stagings(login, connector_name):
    """
    Get all staging tables from a connector.

    Args:
        login: pycarol.Carol
            Carol() instance.
        connector_name: str
            Connector Name

    Returns: list
        list of staging for the connector.

    """

    conn_stats = Connectors(login).stats(connector_name=connector_name)
    st = [i for i in list(conn_stats.values())[0]]
    return sorted(st)


def get_all_etls(login, connector_name):
    """
    get all ETLs from a connector.

    Args:
        login: pycarol.Carol
            Carol() instance.
        connector_name: str
            Connector Name

    Returns: list
        list of ETLs

    """

    connector_id = Connectors(login).get_by_name(connector_name)['mdmId']
    etls = login.call_api(f'v1/etl/connector/{connector_id}', method='GET')
    return etls


def drop_single_etl(login, staging_name, connector_name, output_list, logger):
    """
    Drop ETL based on the outputs of a given ETL.

    Args:
        login: login: pycarol.Carol
            Carol() instance.
        staging_name: str
            staging to drop etls from
        connector_name: str
            connector_name to drop etls from
        output_list: list
            output list of the etl to drop. It will only drop the ETL if all the outputs are present.
        logger: logger
            logger to log process. 

    Returns: None

    """

    if logger is None:
        logger = logging.getLogger(login.domain)

    conn = Connectors(login)
    connector_id = conn.get_by_name(connector_name)['mdmId']
    url = f'v1/etl/connector/{connector_id}/sourceEntity/{staging_name}'
    all_etls = login.call_api(url, )

    for etl in all_etls:
        if len(set(output_list) - set(misc.unroll_list(list(misc.find_keys(etl, 'mdmParameterValues'))))) == 0:
            mdm_id = etl['mdmId']
            logger.info(f'deleting etl {mdm_id} for {staging_name}')
            try:
                # Delete drafts.
                login.call_api(f'v2/etl/{mdm_id}', method='DELETE',
                               params={'entitySpace': 'WORKING'})
            except Exception:
                pass
            login.call_api(f'v2/etl/{mdm_id}', method='DELETE',
                           params={'entitySpace': 'PRODUCTION'})


def drop_etls(login, etl_list):
    """
    Drop ETLs from ETL list.

    Args:
        login: login: pycarol.Carol
            Carol() instance.
        etl_list: list
            list of ETLs to delete.

    Returns: None

    """
    for i in etl_list:
        mdm_id = i['mdmId']
        try:
            # Delete drafts.
            login.call_api(f'v2/etl/{mdm_id}', method='DELETE',
                           params={'entitySpace': 'WORKING'})
        except Exception:
            pass
        login.call_api(f'v2/etl/{mdm_id}', method='DELETE',
                       params={'entitySpace': 'PRODUCTION'})


def par_processing(login, staging_name, connector_name, delete_realtime_records=False,
                   delete_target_folder=False):
    cds_stag = CDSStaging(login)
    n_r = cds_stag.count(staging_name=staging_name,
                         connector_name=connector_name)
    if n_r > 5000000:
        worker_type = 'n1-highmem-16'
        max_number_workers = 16
    else:
        worker_type = 'n1-highmem-4'
        max_number_workers = 16
    number_shards = round(n_r / 100000) + 1
    number_shards = max(16, number_shards)
    task_id = cds_stag.process_data(staging_name=staging_name, connector_name=connector_name, worker_type=worker_type,
                                    number_shards=number_shards, max_number_workers=max_number_workers,
                                    delete_target_folder=delete_target_folder, send_realtime=None,
                                    delete_realtime_records=delete_realtime_records)
    return task_id


def pause_and_clear_subscriptions(login, dm_list, logger):
    if logger is None:
        logger = logging.getLogger(login.domain)

    subs = Subscription(login)

    for idx in dm_list:
        a = subs.get_dm_subscription(idx)

        for dm in a:
            logger.debug(f"Stopping {dm['mdmEntityTemplateName']}")
            subs.pause(dm['mdmId'])
            subs.clear(dm['mdmId'])

    return


def play_subscriptions(login, dm_list, logger):
    if logger is None:
        logger = logging.getLogger(login.domain)

    subs = Subscription(login)

    for idx in dm_list:
        a = subs.get_dm_subscription(idx)

        for dm in a:
            logger.debug(f"Playing {dm['mdmEntityTemplateName']}")
            subs.play(dm['mdmId'])

    return


def find_task_types(login):
    # TODO can user Query from pycarol
    uri = 'v1/queries/filter?indexType=MASTER&scrollable=false&pageSize=1000&offset=0&sortBy=mdmLastUpdated&sortOrder=DESC'

    task_type = ["PROCESS_CDS_STAGING_DATA", "REPROCESS_SEARCH_RESULT"]
    task_status = ["READY", "RUNNING"]

    query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": "mdmTask"},
                          {"mdmKey": "mdmTaskType.raw", "mdmFilterType": "TERMS_FILTER",
                           "mdmValue": task_type},
                          {"mdmKey": "mdmTaskStatus.raw", "mdmFilterType": "TERMS_FILTER",
                           "mdmValue": task_status}]
             }

    r = login.call_api(path=uri, method='POST', data=query)['hits']
    return r


def pause_etls(login, etl_list, connector_name, logger):
    if logger is None:
        logger = logging.getLogger(login.domain)

    r = {}
    conn = Connectors(login)
    for staging_name in etl_list:
        logger.debug(f'Pausing {staging_name} ETLs')
        r[staging_name] = conn.pause_etl(
            connector_name=connector_name, staging_name=staging_name)

    if not all(i['success'] for _, i in r.items()):
        logger.error(f'Some ETLs were not paused. {r}')
        raise ValueError(f'Some ETLs were not paused. {r}')


def pause_single_staging_etl(login, staging_name, connector_name, output_list, logger):

    if logger is None:
        logger = logging.getLogger(login.domain)

    conn = Connectors(login)
    connector_id = conn.get_by_name(connector_name)['mdmId']
    url = f'v1/etl/connector/{connector_id}/sourceEntity/{staging_name}/published'
    all_etls = login.call_api(url, )

    r = []
    for etl in all_etls:
        if len(set(output_list) - set(misc.unroll_list(list(misc.find_keys(etl, 'mdmParameterValues'))))) == 0:
            mdm_id = etl['mdmId']
            logger.info(f'pausing etl {mdm_id} for {staging_name}')

            url = f'v1/etl/{mdm_id}/PRODUCTION/pause'
            resp = login.call_api(url, method='PUT')

            if not resp['success']:
                logger.error(
                    f'Problem starting ETL {connector_name}/{staging_name}\n {resp}')
                raise ValueError(
                    f'Problem starting ETL {connector_name}/{staging_name}\n {resp}')
            r.append(resp)
    return r


def pause_dms(login, dm_list, connector_name, do_not_pause_staging_list=None):

    do_not_pause_staging_list = do_not_pause_staging_list if do_not_pause_staging_list else [
        '']
    conn = Connectors(login)
    mappings = conn.get_dm_mappings(connector_name=connector_name, )
    mappings = mappings = [i['mdmId'] for i in mappings if
                           (i['mdmRunningState'] == 'RUNNING') and
                           (i['mdmMasterEntityName'] in dm_list) and
                           (i['mdmStagingType'] not in do_not_pause_staging_list)
                           ]

    _ = conn.pause_mapping(connector_name=connector_name,
                           entity_mapping_id=mappings)


def par_consolidate(login, staging_name, connector_name, compute_transformations=False, auto_scaling=False):
    cds_stag = CDSStaging(login)
    n_r = cds_stag.count(staging_name=staging_name,
                         connector_name=connector_name)
    if n_r > 5000000:
        worker_type = 'n1-highmem-16'
        max_number_workers = 16
    else:
        worker_type = 'n1-highmem-4'
        max_number_workers = 16
    number_shards = round(n_r / 100000) + 1
    number_shards = max(16, number_shards)
    task_id = cds_stag.consolidate(staging_name=staging_name, connector_name=connector_name, worker_type=worker_type,
                                   compute_transformations=compute_transformations, auto_scaling=auto_scaling,
                                   number_shards=number_shards, rehash_ids=True, max_number_workers=max_number_workers)
    return task_id


def consolidate_stagings(login, connector_name, staging_list, n_jobs=5, compute_transformations=False,
                         auto_scaling=False,
                         logger=None):
    if logger is None:
        logger = logging.getLogger(login.domain)

    task_id = Parallel(n_jobs=n_jobs, backend='threading')(delayed(par_consolidate)(
        login, staging_name=i,
        connector_name=connector_name, auto_scaling=auto_scaling,
        compute_transformations=compute_transformations
    )
        for i in staging_list)

    task_list = [i['data']['mdmId'] for i in task_id]

    return task_list


def par_delete_golden(login, dm_list, n_jobs=5):
    tasks = []

    def del_golden(dm_name, login):
        t = []
        dm_id = DataModel(login).get_by_name(dm_name)['mdmId']
        task = login.call_api("v2/cds/rejected/clearData", method='POST',
                              params={'entityTemplateId': dm_id})['taskId']
        t += [task]
        cds_CDSGolden = CDSGolden(login)
        task = cds_CDSGolden.delete(dm_id=dm_id, )
        delete_golden(login, dm_name)
        t += [task['taskId'], ]
        return t

    tasks = Parallel(n_jobs=n_jobs)(delayed(del_golden)(i, login)
                                    for i in dm_list)
    return list(chain(*tasks))


def par_delete_staging(login, staging_list, connector_name, n_jobs=5):
    tasks = []

    def del_staging(staging_name, connector_name, login):
        t = []
        cds_ = CDSStaging(login)
        task = cds_.delete(staging_name=staging_name,
                           connector_name=connector_name)
        t += [task['taskId'], ]
        return t

    tasks = Parallel(n_jobs=n_jobs)(delayed(del_staging)(
        i, connector_name, login) for i in staging_list)
    return list(chain(*tasks))


def resume_process(login, connector_name, staging_name, logger=None, delay=1):
    if logger is None:
        logger = logging.getLogger(login.domain)

    conn = Connectors(login)
    connector_id = conn.get_by_name(connector_name)['mdmId']

    # TODO Review this once we have mapping and ETLs in the same staging.
    # Play ETLs if any.
    resp = conn.play_etl(connector_id=connector_id, staging_name=staging_name)
    if not resp['success']:
        logger.error(
            f'Problem starting ETL {connector_name}/{staging_name}\n {resp}')
        raise ValueError(
            f'Problem starting ETL {connector_name}/{staging_name}\n {resp}')

    # Play mapping if any.
    mappings_ = check_mapping(login, connector_name, staging_name, )
    if mappings_ is not None:
        # TODO: here assuming only one mapping per staging.
        mappings_ = mappings_[0]
        conn.play_mapping(connector_name=connector_name, entity_mapping_id=mappings_['mdmId'],
                          process_cds=False, )

    # wait for mapping effect.
    time.sleep(delay)
    return mappings_


def check_mapping(login, connector_name, staging_name, logger=None):
    if logger is None:
        logger = logging.getLogger(login.domain)

    conn = Connectors(login)
    resp = conn.get_entity_mappings(connector_name=connector_name, staging_name=staging_name,
                                    errors='ignore'
                                    )

    if isinstance(resp, dict):
        if resp['errorCode'] == 404 and 'Entity mapping not found' in resp['errorMessage']:
            return None
        else:
            logger.error(f'Error checking mapping {resp}')
            raise ValueError(f'Error checking mapping {resp}')

    return resp


def cancel_task_subprocess(login):
    pross_tasks = find_task_types(login)
    pross_task = [i['mdmId'] for i in pross_tasks]
    if pross_task:
        cancel_tasks(login, pross_task)


def check_lookup(login, staging_name, connector_name):
    return Staging(login).get_schema(staging_name=staging_name,
                                     connector_name=connector_name, )['mdmLookupTable']


def change_app_settings(login, app_name, settings, logger=None):

    if logger is None:
        logger = logging.getLogger(login.domain)

    app = Apps(login)
    logger.debug(f'updating settings {settings}')
    s = app.update_setting_values(settings=settings, app_name=app_name)
    return s


def start_app_process(login, app_name, process_name, logger=None):
    """Start a process in Carol

    Args:
        login (pycarol.Carol): Instance of Carol()
        app_name (str): app name to start the process
        process_name (str): Process name. Note, it is case sensitive

    Returns:
        dict: Carol task details.
    """
    if logger is None:
        logger = logging.getLogger(login.domain)

    app = Apps(login)

    a = app.start_app_process(app_name=app_name, process_name=process_name)
    return a


def get_relationships(login, dm_name, entity_space='PRODUCTION',):
    resp = login.call_api(
        path=f'v1/relationship/mapping/direct/name/{dm_name}?entitySpace={entity_space}')
    return resp


def get_relationship_constraints(login, dm_name):
    dm = DataModel(login)
    snap = dm.get_by_name(dm_name)['mdmRelationshipConstraints']
    return snap


def remove_relationships(login, dm_name, publish=True, logger=None):
    """Remove all relationships (normal and constraint) in a data demol

    Args:
        login (pycarol.Carol): Carol instance
        dm_name (str): data model name
        publish (bool, optional): publish data model after delete relationships. Defaults to True.
        logger (logging.Logger, optional): Logger. Defaults to None.

    """

    if logger is None:
        logger = logging.getLogger(login.domain)

    rels = login.call_api(
        path=f'v1/relationship/mapping/direct/name/{dm_name}?entitySpace=PRODUCTION', method='GET')
    rels += get_relationship_constraints(login, dm_name)

    for rel in rels:
        mdm_id = rel['mdmId']
        _ = login.call_api(
            path=f'v1/relationship/mapping/{mdm_id}', method='DELETE')
        logger.debug(f'deleted relationship {mdm_id} in {login.domain}')
        time.sleep(0.2)

    c_dm = CreateDataModel(login)
    dm_id = DataModel(login).get_by_name(dm_name)['mdmId']

    if publish:
        try:
            c_dm.publish_template(dm_id)
        except Exception as e:
            if "Data model was not changed" in str(e):
                pass
            else:
                raise e
        logger.debug(f'Published {dm_name} for {login.domain}')


def remove_relationships_and_delete_data_model(login, dm_name, logger=None):
    """Remove a data model

    Args:
        login (pycarol.Carol): Instance of Carol
        dm_name (str): data model name
        logger (logging.Logger, optional): Logger to log information. Defaults to None.

    Returns:
        dict: carol response
    """

    if logger is None:
        logger = logging.getLogger(login.domain)

    dm = DataModel(login)
    try:
        dm.get_by_name(dm_name)
    except Exception as e:
        if (
            ("Template was not found with the name" in str(e)) or
            ("The entity template is in Deleted state" in str(e))
        ):
            logger.debug(f'`{dm_name}` already deleted in {login.domain}')
            return

    remove_relationships(login, dm_name)
    r = dm.delete(dm_name=dm_name, entity_space='PRODUCTION')
    logger.debug(f'`{dm_name}` deleted in {login.domain}')
    return r


def remove_dms(login, dm_list, logger=None):
    if logger is None:
        logger = logging.getLogger(login.domain)

    for dm_name in dm_list:
        _ = remove_relationships_and_delete_data_model(
            login, dm_name, logger=logger)
        logger.debug(f"{dm_name} dropped from {login.domain}")

    return


def enable_disable_storage_type(login, storage_type, enable, dm_name=None, dm_id=None):
    """Enable or disable a Carol data storage

    Args:
        login (pycarol.Carol): Instance of Carol
        storage_type (str): Possible values: 
            1. 'CDS' Carol Data Storage - Block Storage
            2. 'REALTIME' Realtime Layer - Elasticsearch
            3.  'SQL' Carol Data Lake - SQL Format
        enable (bool): `False` to disable a storage.
        dm_name (str): DataModel name
        dm_id (str): DataModel ID

    Returns:
        dict: Carol response
    """

    if dm_name is not None:
        dm_id = DataModel(login).get_by_name(dm_name)['mdmId']
    elif dm_id is None:
        raise ValueError('Either dm_name or dm_id must be set.')

    url = f'v1/entities/templates/{dm_id}/storageType/{storage_type}'
    payload = {"mdmConsolidationTriggers": [],
               "mdmEnabled": enable, "mdmStorageType": storage_type}
    return login.call_api(url, method='PUT', data=payload)['data']


def disable_all_rt_storage(login, logger=None):

    if logger is None:
        logger = logging.getLogger(login.domain)

    dm = DataModel(login)
    dms = dm.get_all().template_dict
    all_tasks = []
    for dm_name, info in dms.items():
        logger.debug(f'Disable RT for {dm_name} in {login.domain}')
        all_tasks.extend(enable_disable_storage_type(
            login, dm_id=info['mdmId'], storage_type='REALTIME', enable=False))

    return all_tasks


def enable_data_decoration(login):
    c = login.get_current()
    login = copy.deepcopy(login)
    login.switch_org_level()
    info = login.call_api(path=f"v1/tenants/{c['env_id']}", method='GET',)
    info["mdmDataDecorationEnabled"] = True
    info = login.call_api(
        path=f"v1/tenants/{c['env_id']}", method='PUT', data=info)


def change_intake_topic(login, topic):
    c = login.get_current()
    login = copy.deepcopy(login)
    login.switch_org_level()
    info = login.call_api(path=f"v1/tenants/{c['env_id']}", method='GET',)
    info["mdmProcessingTopicOverride"] = topic
    info = login.call_api(
        path=f"v1/tenants/{c['env_id']}", method='PUT', data=info)


def get_snapshot_from_mom(login):
    """Get snapshot from tenant.

    Args:
        login (pycarol.Carol): Carol instance

    Returns:
        dict: mapping definition.
    """
    with login.switch_context(env_name='masterofmaster', org_name='totvstechfindev', app_name="techfinplatform") as masterofmaster:
        staging_name = 'paymentstype'
        connector_id = Connectors(masterofmaster).get_by_name(
            'protheus_carol')['mdmId']
        mapping = get_mapping_from_staing(
            masterofmaster, staging_name, connector_id=connector_id, )
        stag = Staging(masterofmaster)
        mapping_id = mapping[0]['mdmId']
        mappings_to_get = stag.get_mapping_snapshot(
            connector_id=connector_id, mapping_id=mapping_id,)
        mappings_to_get = mappings_to_get.pop(None)
    return mappings_to_get


def get_mapping_from_staing(login, staging_name, connector_name=None, connector_id=None, ):
    """Get publushed mapping information given staging name."

    Args:
        login ([type]): [description]
        staging_name (str): Staging name to send the data.
        connector_name (str, optional): Connector name. Defaults to None.
        connector_id (str, optional): Connector ID. Defaults to None.


    Raises:
        ValueError: [description]

    Returns:
        [list]: list of mappings for that staging.
    """

    if (connector_name is None) and (connector_id is None):
        raise ValueError('Either connector_id or connector_name must be set.')

    connector_id = connector_id or Connectors(
        login).get_by_name(connector_name)['mdmId']
    params = dict(
        reverseMapping=False,
        stagingType=staging_name,
        pageSize=-1,
        sortOrder='ASC'
    )
    mapping = login.call_api(
        f'v1/connectors/{connector_id}/entityMappings/published', method='GET', params=params)['hits']
    return mapping


def create_mapping_from_snapshot(login, mapping_snapshot, connector_id=None, connector_name=None, overwrite=True):
    """Creates a mapping from snapshot

    Args:
        login ([type]): [description]
        mapping_snapshot (dict): Mapping snapshot.
        connector_name (str, optional): Connector name. Defaults to None.
        connector_id (str, optional): Connector ID. Defaults to None.
        overwrite (bool, optional): [description]. Defaults to True.

    Raises:
        ValueError: [description]

    Returns:
        [dict]: Carol's response.
    """
    if (connector_name is None) and (connector_id is None):
        raise ValueError('Either connector_id or connector_name must be set.')

    connector_id = connector_id or Connectors(
        login).get_by_name(connector_name)['mdmId']
    stg_to = Staging(login)
    r = stg_to.mapping_from_snapshot(mapping_snapshot=mapping_snapshot, connector_id=connector_id,
                                     overwrite=overwrite)
    return r


def get_mapping_and_publish(login, connector_name, logger=None):
    if logger is None:
        logger = logging.getLogger(login.domain)
    mapping = get_snapshot_from_mom(login)
    connector_name = 'protheus_carol'
    logger.debug(f'coping mapping tenant {login.domain}')
    create_mapping_from_snapshot(
        login, mapping_snapshot=mapping, connector_name=connector_name)


def get_staging_from_different_tenant(login, staging_name, connector_name, env_name, org_name, app_name,
                                      max_workers=None, columns=None, return_metadata=False, merge_records=True):
    with login.switch_context(env_name=env_name, org_name=org_name, app_name=app_name) as source:
        stag = Staging(source)
        df = stag.fetch_parquet(
            staging_name=staging_name, connector_name=connector_name, max_workers=max_workers,
            columns=columns, merge_records=merge_records,
            return_metadata=return_metadata,
        )

    return df


def send_data_to_tenant_from_source(
        login, staging_name, connector_name, env_name, org_name, app_name,
        max_workers=None, columns=None, return_metadata=False, merge_records=True,  
        async_send=False, step_size=500,
):

    df = get_staging_from_different_tenant(
        login, staging_name, connector_name, env_name, org_name, app_name=app_name,
        max_workers=max_workers, columns=columns, return_metadata=return_metadata, merge_records=merge_records,

    )
    stag = Staging(login)
    stag.send_data(staging_name,
                   data=df,
                   connector_name=connector_name,
                   step_size=step_size,
                   max_workers=max_workers,
                   async_send=async_send,)
