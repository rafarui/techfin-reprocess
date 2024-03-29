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

import argparse

parser = argparse.ArgumentParser(
    description='Update app')
parser.add_argument("-t", '--tenant',
                    type=str,  # required=True,
                    help='tenant domain to update.')

parser.add_argument("-o", '--org',
                    type=str, default='totvstechfin',
                    help='tenant organization to update.')

parser.add_argument('--ignore-sheet', action='store_true',
                    help='Do not use google spreadsheet')

parser.add_argument('--is-painel', action='store_true',
                    help='Is a Painel techfin client')
                    

args = parser.parse_args()

load_dotenv('.env', override=True)


def run(domain, org='totvstechfin', ignore_sheet=False, is_painel=False):
    # avoid all tasks starting at the same time.

    org = org
    app_name = "techfinplatform"
    connector_name = 'protheus_carol'
    connector_group = 'protheus'
    app_version = '0.2.18'

    if ignore_sheet:
        techfin_worksheet = None
    else:
        time.sleep(round(1 + random.random() * 6, 2))
        techfin_worksheet = sheet_utils.get_client()

    process_name = 'processAll'
    app_settings = {'clean_dm': True, 'clean_etls': True, 'skip_pause': False}

    to_drop_stagings = ['se1_acresc', 'cv3_outros',
                        'se1_decresc', 'se2_acresc', 'se2_decresc']

    to_look = ['arInvoices', 'apInvoices',
               'mdCurrencies', 'mdBusinessPartners', ]
    drop_etl_stagings = {
        'se1': [
            {'se1_decresc', },
            {'se1_acresc', }
        ],
        'se2': [
            {'se2_decresc', },
            {'se2_acresc', }

        ]
        }

    drop_data_models = [
        'apbankbearer',
        'apbankbearerlot',
        'appaymentsbank',
        'appaymentscard',
        'appaymentscheckbook',
        'apbankpayment',
        'apcardpayment',
        'apcheckbook',
        'arbankbearer',
        'arbankbearerlot',
        'arpaymentscard',
        'arpaymentscheckbook',
        'arcardpayment',
        'archeckbook',
        'arappayments',
        'cashflowevents',
    ]


    # Create slack handler
    slack_handler = SlackerLogHandler(os.environ["SLACK"], '#techfin-reprocess',  # "@rafael.rui",
                                      username='TechFinBot')
    slack_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    slack_handler.setFormatter(formatter)
    logger = logging.getLogger(domain)
    logger.addHandler(slack_handler)
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

    current_cell = sheet_utils.find_tenant(techfin_worksheet, domain)
    if current_cell is None and ignore_sheet:
        pass
    else:
        current_cell = current_cell.row
        status = techfin_worksheet.row_values(current_cell)[-1].strip().lower()

        skip_status = ['done', 'failed', 'wait',
                       'running', 'installing', 'reprocessing']
        if any(i in status for i in skip_status):
            logger.info(f"Nothing to do in {domain}, status {status}")
            return

    login = carol_login.get_login(domain, org, app_name)
    sheet_utils.update_start_time(techfin_worksheet, current_cell)

    try:
        current_version = carol_apps.get_app_version(
            login, app_name, app_version)
    except:
        logger.error(f"error fetching app version {login.domain}", exc_info=1)
        sheet_utils.update_status(
            techfin_worksheet, current_cell, "failed - fetching app version")
        return


    # Drop DMs
    sheet_utils.update_status(
        techfin_worksheet, current_cell, "running - drop DMs")
    try:
        carol_task.remove_dms(login, drop_data_models)
    except Exception:
        logger.error("error dropping Dms", exc_info=1)
        sheet_utils.update_status(
            techfin_worksheet, current_cell, "failed - dropping Dms")
        return

    # Drop stagings
    sheet_utils.update_status(
        techfin_worksheet, current_cell, "running - drop stagings")

    tasks, fail = carol_task.drop_staging(
        login, staging_list=to_drop_stagings, connector_name=connector_name, logger=logger)
    if fail:
        logger.error(f"error dropping staging {domain}")
        sheet_utils.update_status(
            techfin_worksheet, current_cell, "failed - dropping stagings")
        return
    try:
        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
    except Exception:
        logger.error("error dropping staging", exc_info=1)
        sheet_utils.update_status(
            techfin_worksheet, current_cell, "failed - dropping stagings")
        return

    # Drop ETLs
    sheet_utils.update_status(
        techfin_worksheet, current_cell, "running - drop ETLs")
    for key, values in drop_etl_stagings.items():
        for value in values:
            try:
                carol_task.drop_single_etl(login=login, staging_name=key, connector_name=connector_name,
                                           output_list=value, logger=logger)
            except:
                logger.error("error dropping ETLs", exc_info=1)
                sheet_utils.update_status(
                    techfin_worksheet, current_cell, "failed - dropping ETLs")
                return

    fail = False
    task_list = '__unk__'
    if current_version != app_version:

        logger.info(f"Updating app from {current_version} to {app_version}")
        sheet_utils.update_version(
            techfin_worksheet, current_cell, current_version)
        sheet_utils.update_status(
            techfin_worksheet, current_cell, "running - app install")
        task_list, fail = carol_apps.update_app(
            login, app_name, app_version, logger, connector_group=connector_group)
        sheet_utils.update_version(
            techfin_worksheet, current_cell, app_version)
    else:
        logger.info(f"Running version {app_version}")
        sheet_utils.update_version(
            techfin_worksheet, current_cell, app_version)
        # return

    if fail:
        sheet_utils.update_status(techfin_worksheet, current_cell,
                                  'failed - app install')

        return

    # Cancel unwanted tasks.
    sheet_utils.update_status(
        techfin_worksheet, current_cell, "running - canceling tasks")
    pross_tasks = carol_task.find_task_types(login)
    pross_task = [i['mdmId'] for i in pross_tasks]
    if pross_task:
        carol_task.cancel_tasks(login, pross_task)


    use_process_all = sheet_utils.get_col5(techfin_worksheet, current_cell) or ''

    if use_process_all.lower() != 'false':
        sync_type = sheet_utils.get_sync_type(
            techfin_worksheet, current_cell) or ''
        if 'painel' in sync_type.lower().strip() or is_painel:
            # deleting all data from techfin
            sheet_utils.update_status(
                techfin_worksheet, current_cell, "running - deleting DM from techfin")

            try:
                r = techfin_task.delete_and_track(login.domain, to_look=to_look, )
            except Exception:
                logger.error("failed - deleting DM from techfin", exc_info=1)
                sheet_utils.update_status(
                    techfin_worksheet, current_cell, "failed - deleting DM from techfin")
                return
            if r:
                logger.error("failed - deleting DM from techfin",)
                sheet_utils.update_status(
                    techfin_worksheet, current_cell, "failed - deleting DM from techfin")
                return

        # prepare process All
        sheet_utils.update_status(
            techfin_worksheet, current_cell, "running - processAll")
        carol_task.change_app_settings(
            login=login, app_name=app_name, settings=app_settings)

        task = carol_task.start_app_process(
            login, app_name=app_name, process_name=process_name)
        tasks = [task['data']['mdmId']]
        try:
            task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
        except Exception:
            logger.error("failed - processAll", exc_info=1)
            sheet_utils.update_status(
                techfin_worksheet, current_cell, "failed - processAll")
            return
        if fail:
            logger.info(f"'failed - processAll'")
            sheet_utils.update_status(
                techfin_worksheet, current_cell, "failed - processAll")
            return
    else: 
        logger.info(f"not running processAll")

    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(techfin_worksheet, current_cell, "Done")
    sheet_utils.update_end_time(techfin_worksheet, current_cell)

    return task_list


if __name__ == "__main__":
    techfin_worksheet = sheet_utils.get_client()

    if args.tenant is not None:
        ignore_sheet = args.ignore_sheet
        is_painel = args.is_painel
        run(args.tenant, org=args.org, ignore_sheet=ignore_sheet, is_painel=is_painel)

    else:
        has_tenant = [1, 2, 3]
        while len(has_tenant) > 1:
            table = techfin_worksheet.get_all_records()
            skip_status = ['done', 'failed', 'running',
                           'installing', 'reprocessing', 'wait']
            to_process = [t['environmentName (tenantID)'].strip() for t in table
                          if t.get('environmentName (tenantID)', None) is not None
                          and t.get('environmentName (tenantID)', 'None') != ''
                          and not any(i in t.get('Status', '').lower().strip() for i in skip_status)
                          ]

            has_tenant = [i for i in table if i['Status']
                          == '' or i['Status'] == 'wait']
            print(
                f"there are {len(to_process)} to process and {len(has_tenant)} waiting")

            pool = multiprocessing.Pool(5)
            pool.map(run, to_process)
            pool.close()
            pool.join()

            time.sleep(240)
