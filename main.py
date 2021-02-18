from pycarol import Carol, ApiKeyAuth, PwdAuth, CDSStaging, Connectors
from pycarol import Tasks
import random
import time
import os
from dotenv import load_dotenv
from joblib import Parallel, delayed
from functions import sheet_utils, carol_login, carol_apps, carol_task, custom_pipeline, techfin_task
import argparse
from slacker_log_handler import SlackerLogHandler
import logging
from functools import reduce
import multiprocessing

load_dotenv('.env', override=True)


def run(domain, org='totvstechfin'):
    # avoid all tasks starting at the same time.
    time.sleep(round(3 + random.random() * 6, 2))
    org = 'totvstechfin'
    app_name = "techfinplatform"
    app_version = '0.0.80'
    connector_name = 'protheus_carol'
    connector_group = 'protheus'

    dms = ['arinvoiceinstallment','apinvoiceinstallment', 'arinvoicepayments','apinvoicepayments', 'cfbankbalance']
    staging_list = ['se1_installments', 'se2_installments', 'fk1', 'fk2', 'se8'] 

    techfin_worksheet = sheet_utils.get_client()

    # Create slack handler
    # slack_handler = SlackerLogHandler(os.environ["SLACK"], '#techfin-reprocess',  # "@rafael.rui",
                                    #   username='TechFinBot')
    # slack_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # slack_handler.setFormatter(formatter)
    logger = logging.getLogger(domain)
    # logger.addHandler(slack_handler)
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

    current_cell = sheet_utils.find_tenant(techfin_worksheet, domain)
    status = techfin_worksheet.row_values(current_cell.row)[-1].strip().lower()

    skip_status = ['done', 'failed', 'wait', 'running', 'installing', 'reprocessing']
    if any(i in status for i in skip_status):
        logger.info(f"Nothing to do in {domain}, status {status}")
        return

    login = carol_login.get_login(domain, org, app_name)
    sheet_utils.update_start_time(techfin_worksheet, current_cell.row)


    current_version = carol_apps.get_app_version(login, app_name, app_version)
    fail = False
    task_list = '__unk__'
    if current_version != app_version:

        # Stop pub/sub if any.
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "running - stop pubsub")
        try:
            carol_task.pause_and_clear_subscriptions(login, dms, logger)
        except Exception as e:
            logger.error("error stop pubsub", exc_info=1)
            sheet_utils.update_status(techfin_worksheet, current_cell.row, "failed - stop pubsub")
            return

        logger.info(f"Updating app from {current_version} to {app_version}")
        sheet_utils.update_version(techfin_worksheet, current_cell.row, current_version)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "running - app install")
        task_list, fail = carol_apps.update_app(login, app_name, app_version, logger, connector_group=connector_group)
        sheet_utils.update_version(techfin_worksheet, current_cell.row, app_version)
    else:
        logger.info(f"Running version {app_version}")
        sheet_utils.update_version(techfin_worksheet, current_cell.row, app_version)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "Done")
        sheet_utils.update_end_time(techfin_worksheet, current_cell.row)
        return

    if fail:
        sheet_utils.update_status(techfin_worksheet, current_cell.row,
                                  'failed - app install')
        return

    # Stop pub/sub if any.
    sheet_utils.update_status(techfin_worksheet, current_cell.row, "running - stop pubsub")
    try:
        carol_task.pause_and_clear_subscriptions(login, dms, logger)
    except Exception as e:
        logger.error("error stop pubsub", exc_info=1)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "failed - stop pubsub")
        return
    try:
        carol_task.play_subscriptions(login, dms, logger)
    except Exception as e:
        logger.error("error playing pubsub", exc_info=1)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "failed - playing pubsub")
        return


    sheet_utils.update_status(techfin_worksheet, current_cell.row, "running - processing")
    tasks = []
    try:
        for staging_name in staging_list:
            logger.debug(f"processing {staging_name}")
            task_id = CDSStaging(login).process_data(staging_name, connector_name=connector_name, max_number_workers=16,
                                                     delete_target_folder=False, delete_realtime_records=False,
                                                     recursive_processing=False, auto_scaling=False)
            tasks += [task_id['data']['mdmId']]

        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
    except Exception:
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after processing", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after processing")
        return


    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(techfin_worksheet, current_cell.row, "Done")
    sheet_utils.update_end_time(techfin_worksheet, current_cell.row)

    return task_list


if __name__ == "__main__":
    techfin_worksheet = sheet_utils.get_client()

    #     run("tenant626b1cec914111eabf8a0a5864600195")

    has_tenant = [1, 2, 3]
    while len(has_tenant) > 1:
        table = techfin_worksheet.get_all_records()
        skip_status = ['done', 'failed', 'running', 'installing', 'reprocessing', 'wait']
        to_process = [t['environmentName (tenantID)'].strip() for t in table
                      if t.get('environmentName (tenantID)', None) is not None
                      and t.get('environmentName (tenantID)', 'None') != ''
                      and not any(i in t.get('Status', '').lower().strip() for i in skip_status)
                      ]

        has_tenant = [i for i in table if i['Status'] == '' or i['Status'] == 'wait']
        print(f"there are {len(to_process)} to process and {len(has_tenant)} waiting")

        pool = multiprocessing.Pool(5)
        pool.map(run, to_process)
        pool.close()
        pool.join()

        time.sleep(240)

