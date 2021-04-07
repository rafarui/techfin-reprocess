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


def run(domain, org='totvstechfin', ):
    # avoid all tasks starting at the same time.
    time.sleep(round(1 + random.random() * 6, 2))
    org = org
    app_name = "techfinplatform"
    connector_name = 'protheus_carol'
    connector_group = 'protheus'
    app_version = '0.2.7'
    techfin_worksheet = sheet_utils.get_client()

    to_drop_stagings = ['se1_acresc', 'cv3_outros', 
                        'se1_decresc', 'se2_acresc', 'se2_decresc']

    consolidate_list = [
        'cv3_entrada_creditos',
        'cv3_entrada_debitos',
        'cv3_saida_creditos',
        'cv3_saida_debitos',
        'se1',
        'se2',
        'ar1_2',
        'sf2',
        'sf1',
        'sd1',
        'cvd',
        'fk2',
        'fkd_deletado',
        'fk5_estorno_transferencia_pagamento',
        'fk1',
        'fk5_transferencia',
        'sea_1_frv_descontado_naodeletado_invoicepayment',
        'fkd_1',
        'sea_1_frv_descontado_deletado_invoicepayment',

    ]

    to_del_staging = [
        'sf1_consulta', 'sf1_invoicebra', 'sf2_consulta', 'sf2_invoicebra',
        'sd1_consulta', 'sd1_dados', 'sd1_devolution',
        'sd1_else', 'se1_installments', 'se1_invoice', 'se1_payments',
        'se1_payments_abatimentos', 'se2_installments',  'se2_invoice',
        'se2_payments', 'se2_payments_abatimentos',
        'cvd_else', 'cvd_contas_avaliadas',
    ]

    to_del_dms = ['apinvoiceaccounting',
                  'apinvoice',
                  'arinvoice',
                  'arinvoiceinstallment',
                  'apinvoicepayments',
                  'arinvoicebra',
                  'arinvoiceaccounting',
                  'apinvoiceinstallment',
                  'apinvoicebra',
                  'arinvoiceorigin',
                  'arinvoicepartner',
                  'arinvoicepayments',

                  ]

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

    pause_etl_stagings = {
        'se1': [
            {'se1_installments',
             'se1_invoice',
             'se1_payments',
             'se1_payments_abatimentos'},
        ],
        'se2': [
            {'se2_installments',
             'se2_invoice',
             'se2_payments',
             'se2_payments_abatimentos'},

        ]}

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

    current_cell = sheet_utils.find_tenant(techfin_worksheet, domain)
    status = techfin_worksheet.row_values(current_cell.row)[-1].strip().lower()

    skip_status = ['done', 'failed', 'wait',
                   'running', 'installing', 'reprocessing']
    if any(i in status for i in skip_status):
        logger.info(f"Nothing to do in {domain}, status {status}")
        return

    login = carol_login.get_login(domain, org, app_name)
    sheet_utils.update_start_time(techfin_worksheet, current_cell.row)

    dag = list(reduce(set.union, custom_pipeline.get_dag()))
    dms = [i.replace('DM_', '') for i in dag if i.startswith('DM_')]

    try:
        current_version = carol_apps.get_app_version(
            login, app_name, app_version)
    except:
        logger.error(f"error fetching app version {login.domain}", exc_info=1)
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - fetching app version")
        return

    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - drop stagings")

    tasks, fail = carol_task.drop_staging(
        login, staging_list=to_drop_stagings, connector_name=connector_name, logger=logger)
    if fail:
        logger.error(f"error dropping staging {domain}")
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - dropping stagings")
        return
    try:
        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)
    except Exception as e:
        logger.error("error dropping staging", exc_info=1)
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - dropping stagings")
        return

    # Drop ETLs
    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - drop ETLs")
    for key, values in drop_etl_stagings.items():
        for value in values:
            try:
                carol_task.drop_single_etl(login=login, staging_name=key, connector_name=connector_name,
                                           output_list=value, logger=logger)
            except:
                logger.error("error dropping ETLs", exc_info=1)
                sheet_utils.update_status(
                    techfin_worksheet, current_cell.row, "failed - dropping ETLs")
                return

    fail = False
    task_list = '__unk__'
    if current_version != app_version:

        logger.info(f"Updating app from {current_version} to {app_version}")
        sheet_utils.update_version(
            techfin_worksheet, current_cell.row, current_version)
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "running - app install")
        task_list, fail = carol_apps.update_app(
            login, app_name, app_version, logger, connector_group=connector_group)
        sheet_utils.update_version(
            techfin_worksheet, current_cell.row, app_version)
    else:
        logger.info(f"Running version {app_version}")
        sheet_utils.update_version(
            techfin_worksheet, current_cell.row, app_version)
        sheet_utils.update_status(techfin_worksheet, current_cell.row, "Done")
        sheet_utils.update_end_time(techfin_worksheet, current_cell.row)
        # return

    if fail:
        sheet_utils.update_status(techfin_worksheet, current_cell.row,
                                  'failed - app install')

        return

    # Cancel unwanted tasks.
    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - canceling tasks")
    pross_tasks = carol_task.find_task_types(login)
    pross_task = [i['mdmId'] for i in pross_tasks]
    if pross_task:
        carol_task.cancel_tasks(login, pross_task)

    # pause ETLs.
    for key, values in pause_etl_stagings.items():
        for value in values:
            try:
                carol_task.pause_single_staging_etl(
                    login=login, staging_name=key, connector_name=connector_name, output_list=value, logger=logger
                )
            except:
                sheet_utils.update_status(techfin_worksheet, current_cell.row,
                                          'failed - stopping ETLs')
                return

    # pause mappings.
    carol_task.pause_dms(login, dm_list=dms, connector_name=connector_name,
                         do_not_pause_staging_list=do_not_pause_staging_list)
    time.sleep(round(10 + random.random() * 6, 2))  # pause have affect

    # Delete all invoice-accountings from techfin

    sync_type = sheet_utils.get_sync_type(techfin_worksheet, current_cell.row) or ''
    if 'painel' in sync_type.lower().strip():
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "running - delete invoice-accountings techfin")
        try:
            res = techfin_task.delete_invoice_accountings(login.domain)
        except Exception as e:
            sheet_utils.update_status(
                techfin_worksheet, current_cell.row, "failed - delete invoice-accountings techfin")
            logger.error(
                "error after delete invoice-accountings techfin", exc_info=1)
            return

    # consolidate
    logger.debug(f"running - consolidate for {domain}")
    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - consolidate")
    task_list = carol_task.consolidate_stagings(login, connector_name=connector_name, staging_list=consolidate_list,
                                                n_jobs=1, logger=logger, auto_scaling=auto_scaling,
                                                compute_transformations=compute_transformations)

    try:
        task_list, fail = carol_task.track_tasks(
            login, task_list, logger=logger)
    except:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after consolidate", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - consolidate")
        logger.error("error after consolidate")
        return

    # delete stagings.
    logger.debug(f"running -  delete stagings {domain}")
    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - delete stagings")

    task_list = carol_task.par_delete_staging(
        login, staging_list=to_del_staging, connector_name=connector_name, n_jobs=1)
    try:
        task_list, fail = carol_task.track_tasks(
            login, task_list, logger=logger)
    except:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - delete stagings")
        logger.error("error after delete DMs", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - delete stagings")
        logger.error("error after delete DMs")
        return

    # delete DMs
    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - delete DMs")
    task_list = carol_task.par_delete_golden(
        login, dm_list=to_del_dms, n_jobs=1)
    try:
        task_list, fail = carol_task.track_tasks(
            login, task_list, logger=logger)
    except:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - delete DMs")
        logger.error("error after delete DMs", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - delete DMs")
        logger.error("error after delete DMs")
        return

    sheet_utils.update_status(
        techfin_worksheet, current_cell.row, "running - processing")
    try:
        fail = custom_pipeline.run_custom_pipeline(
            login, connector_name=connector_name, logger=logger)
    except Exception:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after processing", exc_info=1)
        return
    if fail:
        sheet_utils.update_status(
            techfin_worksheet, current_cell.row, "failed - processing")
        logger.error("error after processing")
        return

    logger.info(f"Finished all process {domain}")
    sheet_utils.update_status(techfin_worksheet, current_cell.row, "Done")
    sheet_utils.update_end_time(techfin_worksheet, current_cell.row)

    return task_list


if __name__ == "__main__":
    techfin_worksheet = sheet_utils.get_client()

    # run("tenant88889a28b7a211eab6fd0a58646001d7", org='totvstechfin')

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
