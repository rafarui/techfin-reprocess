from pycarol import Carol, ApiKeyAuth, PwdAuth, Tasks
import os

from . import sheet_utils


def get_login(domain, org):
    email = os.environ['CAROLUSER']
    password = os.environ['CAROLPWD']
    carol_app = 'techfinplatform'
    login = Carol(domain, carol_app, auth=PwdAuth(email, password), organization=org, )
    api_key = login.issue_api_key()

    login = Carol(domain, carol_app, auth=ApiKeyAuth(api_key['X-Auth-Key']),
                  connector_id=api_key['X-Auth-ConnectorId'], organization=org, )
    return login

def update_app(login, app_name, app_version, logger):


    current_cell = sheet_utils.find_tenant(sheet_utils.techfin_worksheet, login.domain)

    #check if stall task is running.
    uri = 'v1/queries/filter?indexType=MASTER&scrollable=false&pageSize=25&offset=0&sortBy=mdmLastUpdated&sortOrder=DESC'

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
            task_list, fail = track_tasks(login, [task_id], logger=logger)
            if installing_version == app_version:
                return task_list, False
        except Exception as e:
            logger.error("error fetching already running task, will try again", exc_info=1)
            fail = True

    to_install = login.call_api("v1/tenantApps/subscribableCarolApps", method='GET')
    to_install = [i for i in to_install['hits'] if i["mdmName"] == app_name]
    if to_install:
        to_install = to_install[0]
        assert to_install["mdmAppVersion"] == app_version
        to_install_id = to_install['mdmId']
        updated = login.call_api(f"v1/tenantApps/subscribe/carolApps/{to_install_id}", method='POST')
        params = {"publish": True, "connectorGroup": "protheus"}
        install_task = login.call_api(f"v1/tenantApps/{updated['mdmId']}/install", method='POST', params=params)
        install_task = install_task['mdmId']
    else:
        #check failed task
        task = check_failed_instalL(login, app_name, app_version)
        if task:
            install_task = login.call_api(f'v1/tasks/{task}/reprocess', method="POST")['mdmId']
        else:
            logger.error("Error trying to update app")
            sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED - did not found app to install')
            return [], True


    task_list = []
    try:
        task_list, fail = track_tasks(login, [install_task], logger=logger)
    except Exception as e:
        logger.error("error after app install", exc_info=1)
        fail = True

    if fail:
        logger.error(f"Problem with {login.domain} during App installation.")
        sheet_utils.update_task_id(techfin_worksheet, current_cell.row, install_task)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, 'FAILED - app install')
        return [], fail

    return task_list, False