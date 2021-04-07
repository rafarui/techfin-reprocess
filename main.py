from pycarol import Carol, ApiKeyAuth, PwdAuth, CDSStaging, Connectors
from pycarol import Tasks
import random
import time
import os
from dotenv import load_dotenv
from joblib import Parallel, delayed
from functions import carol_login, carol_apps, carol_task, custom_pipeline, techfin_task
import argparse
from slacker_log_handler import SlackerLogHandler
import logging
from functools import reduce
import multiprocessing

load_dotenv('.env', override=True)


def run(domain, org='totvstechfin', ):
    # avoid all tasks starting at the same time.
    time.sleep(round(1 + random.random() * 6, 2))
    org = org
    app_name = "techfinplatform"
    connector_name = 'protheus_carol'
    connector_group = 'protheus'
    app_version = '0.2.7'
    process_name = 'processAll'

    app_settings = {'clean_dm': True, 'clean_etls': True, 'skip_pause': False}

    to_drop_stagings = ['se1_acresc', 'cv3_outros',
                        'se1_decresc', 'se2_acresc', 'se2_decresc']

    drop_etl_stagings = {
        'se1': [
            {'se1_decresc', },
            {'se1_acresc', }
        ],
        'se2': [
            {'se2_decresc', },
            {'se2_acresc', }

        ]}

    do_not_pause_staging_list = None

    # need to force the old data to the stagings transformation.
    compute_transformations = True
    auto_scaling = True

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

    login = carol_login.get_login(domain, org, app_name)

    try:
        current_version = carol_apps.get_app_version(
            login, app_name, app_version)
    except:
        logger.error(f"error fetching app version {login.domain}", exc_info=1)
        return

    tasks, fail = carol_task.drop_staging(
        login, staging_list=to_drop_stagings, connector_name=connector_name, logger=logger)
    if fail:
        logger.error(f"error dropping staging {domain}")
        return

    try:
        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
    except Exception as e:
        logger.error("error dropping staging", exc_info=1)
        return

    # Drop ETLs
    for key, values in drop_etl_stagings.items():
        for value in values:
            try:
                carol_task.drop_single_etl(login=login, staging_name=key, connector_name=connector_name,
                                           output_list=value, logger=logger)
            except:
                logger.error("error dropping ETLs", exc_info=1)
                return

    fail = False
    task_list = '__unk__'
    if current_version != app_version:
        logger.info(f"Updating app from {current_version} to {app_version}")
        task_list, fail = carol_apps.update_app(
            login, app_name, app_version, logger, connector_group=connector_group)
    else:
        logger.info(f"Running version {app_version}")
        # return

    if fail:
        logger.info(f"'failed - app install'")
        return

    # Cancel unwanted tasks.
    pross_tasks = carol_task.find_task_types(login)
    pross_task = [i['mdmId'] for i in pross_tasks]
    if pross_task:
        carol_task.cancel_tasks(login, pross_task)

    # prepare process All
    carol_task.change_app_settings(
        login=login, app_name=app_name, settings=app_settings)

    task = carol_task.start_app_process(
        login, app_name=app_name, process_name=process_name)
    tasks = [task['data']['mdmId']]
    try:
        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
    except Exception as e:
        logger.error("failed - processAll", exc_info=1)
        return
    if fail:
        logger.info(f"'failed - processAll'")
        return

    logger.info(f"Finished all process {domain}")

    return task_list


if __name__ == "__main__":

    run("tenant88889a28b7a211eab6fd0a58646001d7", org='totvstechfin')
