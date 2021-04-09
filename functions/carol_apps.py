from pycarol import Carol, ApiKeyAuth, PwdAuth, Tasks, Apps
from functools import partial
from . import carol_task

def get_app_version(login, app_name, version=None):
    app = Apps(login)
    app_info = app.get_by_name(app_name)
    return app_info['mdmAppVersion']

def check_failed_instalL(login, app_name, app_version):
    app = login.call_api("v1/tenantApps?pageSize=-1", method='GET')
    app = [i for i in app['hits'] if (i["mdmName"] == app_name and
                                      i["mdmAppVersion"] == app_version and
                                      i['mdmInstallationTaskStatus'] == 'FAILED')]
    if app:
        app = app[0]['mdmInstallationTaskId']
        return app

def update_app(login, app_name, app_version, logger, connector_group=None):

    #check if there is a install task is running.
    uri = 'v1/queries/filter?indexType=MASTER&scrollable=false&pageSize=25&offset=0&sortBy=mdmLastUpdated&sortOrder=DESC'
    # TODO can user Query from pycarol
    query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": "mdmTask"},
                          {"mdmKey": "mdmTaskType.raw", "mdmFilterType": "TERMS_FILTER",
                           "mdmValue": ["INSTALL_CAROL_APP"]},
                          {"mdmKey": "mdmTaskStatus.raw", "mdmFilterType": "TERMS_FILTER", "mdmValue": ["RUNNING"]}],
             "mustNotList": [{"mdmKey": "mdmUserId.raw", "mdmFilterType": "MATCH_FILTER", "mdmValue": ""}],
             "shouldList": []}

    r = login.call_api(path=uri, method='POST', data=query)
    if len(r['hits']) >= 1:
        logger.info(f'Found install task in {login.domain}')
        task_id = r['hits'][0]['mdmId']
        installing_version = r['hits'][0]['mdmData']['carolAppVersion']
        try:
            callback = partial(carol_task.cancel_task_subprocess, login=login)
            task_list, fail = carol_task.track_tasks(login, [task_id], logger=logger, callback=callback)
            if installing_version == app_version:
                return task_id, False
        except Exception as e:
            logger.error("error fetching already running task, will try again", exc_info=1)

    to_install = login.call_api("v1/tenantApps/subscribableCarolApps", method='GET')
    to_install = [i for i in to_install['hits'] if i["mdmName"] == app_name]
    if to_install:
        to_install = to_install[0]
        assert to_install["mdmAppVersion"] == app_version
        to_install_id = to_install['mdmId']
        updated = login.call_api(f"v1/tenantApps/subscribe/carolApps/{to_install_id}", method='POST')
        params = {"publish": True, "connectorGroup": connector_group}
        install_task = login.call_api(f"v1/tenantApps/{updated['mdmId']}/install", method='POST', params=params)
        install_task = install_task['mdmId']
    else:
        #check failed task
        task = check_failed_instalL(login, app_name, app_version)
        if task:
            install_task = login.call_api(f'v1/tasks/{task}/reprocess', method="POST")['mdmId']
        else:
            logger.error("Error trying to update app")
            return '__unk__', True

    try:
        callback = partial(carol_task.cancel_task_subprocess, login=login)
        task_list, fail = carol_task.track_tasks(login, [install_task], logger=logger, callback=callback)
    except Exception as e:
        logger.error("error after app install", exc_info=1)
        fail = True

    if fail:
        logger.error(f"Problem with {login.domain} during App installation task = {install_task}.")
        return install_task, fail

    return install_task, False



