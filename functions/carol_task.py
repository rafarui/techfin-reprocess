from pycarol import (
    Carol, ApiKeyAuth, PwdAuth, Tasks, Staging, Connectors, CDSStaging, Subscription, DataModel
)

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


def cancel_tasks(login, task_list, logger=None):
    if logger is None:
        logger = logging.getLogger(login.domain)

    carol_task = Tasks(login)
    for i in task_list:
        logger.debug(f"Canceling {i}")
        carol_task.cancel(task_id=i, force=True)

    return


def track_tasks(login, task_list, do_not_retry=False, logger=None, callback=None):
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
            if do_not_retry:
                logger.error(f'Task: {task} failed. It wll not be restarted.')
                continue
            if retry_tasks[task] > 3:
                max_retries.update([task])
                logger.error(f'Task: {task} failed 3 times. will not restart')
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

    Args:
        login: login: pycarol.Carol
            Carol() instance.
        staging_name: str
            staging to drop etls from
        connector_name: str
            connector_name to drop etls from
        output_list: list
            output list of the etl to drop
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
            except Exception as e:
                pass
            login.call_api(f'v2/etl/{mdm_id}', method='DELETE',
                           params={'entitySpace': 'PRODUCTION'})

def drop_etls(login, etl_list):
    """

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
        except Exception as e:
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

    r = conn.pause_mapping(connector_name=connector_name,
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