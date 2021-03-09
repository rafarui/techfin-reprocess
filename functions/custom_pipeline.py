from toposort import toposort_flatten, toposort
from . import carol_task
from pycarol import CDSStaging, Connectors
from functools import reduce
import logging
import time


def get_dag():

    rel = {}

    # Aps
    rel['se2_invoice'] = ['se2', ]
    rel['se2_payments'] = ['se2', 'DM_apinvoiceinstallment', ]

    rel['DM_apinvoicepayments'] = ['se2_payments_abatimentos',
                                   'DM_apinvoiceinstallment',
                                   'se2_payments', ]

    rel['se2_payments_abatimentos'] = ['se2', 'DM_apinvoiceinstallment', ]

    rel['DM_apinvoiceinstallment'] = ['se2_installments', 'DM_apinvoice', ]

    rel['se2_installments'] = [
        'se2',
        'DM_apinvoice', ]

    rel['DM_apinvoicebra'] = ['sf1_invoicebra', 'DM_apinvoice', ]

    rel['sf1_invoicebra'] = [
        'DM_apinvoice',
        'sf1', ]

    rel['DM_apinvoiceaccounting'] = [
        'cv3_entrada_creditos', 'DM_apinvoice', 'cv3_entrada_debitos']
    rel['cv3_entrada_creditos'] = [
        'sf1_consulta',
        'DM_apinvoice',
        'sd1_consulta',
    ]

    rel['cv3_entrada_debitos'] = [
        'sf1_consulta',
        'DM_apinvoice',
        'sd1_consulta', ]

    rel['sf1_consulta'] = ['sf1']
    rel['sd1_consulta'] = ['sd1']
    rel['DM_apinvoice'] = ['se2_invoice', ]

    # ARs
    rel['DM_arinvoiceorigin'] = ['DM_arinvoice', 'sd1_devolution']

    rel['sd1_dados'] = ['sd1']

    rel['sd1_devolution'] = [
        'sf2_consulta',
        'sd1_dados',
        'DM_arinvoice',
        'sf1_consulta',
    ]

    rel['sf1_consulta'] = ['sf1']

    rel['cv3_saida_debitos'] = [
        'sf2_consulta',
        'DM_arinvoice',
    ]

    rel['DM_arinvoiceaccounting'] = [
        'cv3_saida_creditos', 'DM_arinvoice', 'cv3_saida_debitos']
    rel['cv3_saida_creditos'] = [
        'sf2_consulta',
        'DM_arinvoice', ]

    rel['DM_arinvoicepartner'] = ['DM_arinvoice', 'ar1_2']

    rel['ar1_2'] = [
        'sf2_consulta',
        'DM_arinvoice',
    ]

    rel['sf2_consulta'] = ['sf2']

    rel['DM_arinvoicepayments'] = ['DM_arinvoiceinstallment',
                                   'se1_payments_abatimentos',
                                   'se1_payments',
                                   ]

    rel['DM_arinvoiceinstallment'] = ['DM_arinvoice', 'se1_installments']
    rel['DM_arinvoice'] = ['se1_invoice']
    rel['se1_invoice'] = ['se1']

    rel['se1_installments'] = [
        'DM_arinvoice',
        'se1',
    ]
    rel['se1_payments_abatimentos'] = ['se1', 'DM_arinvoiceinstallment', ]

    rel['se1_payments'] = ['se1', 'DM_arinvoiceinstallment', ]

    rel['DM_arinvoicebra'] = ['sf2_invoicebra', 'DM_arinvoice', ]

    rel['sf2_invoicebra'] = [
        'DM_arinvoice',
        'sf2',
    ]
    rel = {i: set(j) for i, j in rel.items()}
    dag_order = toposort(rel)

    return dag_order


def run_custom_pipeline(login, connector_name, logger):

    if logger is None:
        logger = logging.getLogger(login.domain)
    dag = get_dag()

    for p in dag:
        dm = [i for i in p if i.startswith('DM_')]
        stagings = [i for i in p if not i.startswith('DM_')]
        tasks = []
        if dm:
            # play integration
            # TODO: reprocess rejected?
            pass

        for staging_name in stagings:
            mappings_ = carol_task.resume_process(login, connector_name=connector_name,
                                                  staging_name=staging_name, logger=logger, delay=1)
        # wait for play.
        time.sleep(120)

        for staging_name in stagings:
            logger.debug(f"processing {staging_name}")
            task_id = CDSStaging(login).process_data(staging_name, connector_name=connector_name, max_number_workers=16,
                                                     delete_target_folder=False, delete_realtime_records=False,
                                                     recursive_processing=False, auto_scaling=False)
            tasks += [task_id['data']['mdmId']]

        task_list, fail = carol_task.track_tasks(login, tasks, logger=logger)

        if fail:
            return True

    return False
