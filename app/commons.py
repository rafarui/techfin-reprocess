from pycarol.pipeline import Task, inherit_list, ParquetTarget
import traceback
import logging
import time
import json
import os
import datetime
import pandas as pd
import numpy as np
import sys
import os
import shutil
import luigi

luigi.interface.InterfaceLogging.setup(luigi.interface.core())

logger = logging.getLogger(__name__)


PROJECT_PATH = os.getcwd()
TARGET_PATH = os.path.join(PROJECT_PATH, 'luigi_targets')
Task.TARGET_DIR = TARGET_PATH
Task.is_cloud_target = False
Task.tenant = luigi.Parameter()
Task.organization = luigi.Parameter()
Task.app_name = luigi.Parameter()
Task.app_version = luigi.Parameter()

# Task.resources = {'cpu': 1}


@Task.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """

    if isinstance(exception, NoMedicalFormToPredict):
        logger.error(f'No Medical form to predict. Check integration.')
        return

    logger.error(f'Error msg: {exception} ---- Error: Task {task},')
    traceback_str = ''.join(traceback.format_tb(exception.__traceback__))
    logger.error(traceback_str)


@Task.event_handler(luigi.Event.PROCESSING_TIME)
def print_execution_time(self, processing_time):
    logger.debug(
        f'### PROCESSING TIME {processing_time}s. Output saved at {self.output().path}')


#######################################################################################################

tenant_list = [
    "tenant0f6e9113133911eb8c540a5864606c4e", "tenant0ae98dfe3ef511eb8d560a5864606c7b", "tenant2d8f45f1cdf411ea93610a586460a81b",
    "tenantf6ea21aa445511eb950a0a58646001ad", "tenantd567dd0d81c64d64922ddea5fea82e13", "tenant70070bc83e3911eb89030a5864602a96",
    "tenant6cdc35503fc511eb8d560a5864606c7b", "tenant134f2f1e67b511eb8f7d0a586460016c", "tenant13ad0d06e1b542deba9c489d3efe587a",
    "tenant04484c30769a11eba0e30a5864602aee", "tenantb7121fea6c6f11ebb2be0a58646027aa", "tenantfce00b6467cd11ebb5ef0a5864613f9c",
    "tenant9c553a3c76a311ebaedc0a586461459f", "tenant8462c85ec0a311ea84620a58646085f8", "tenantb269207f206d11eb8a1e0a5864613fef",
    "tenant18a012ed67b411ebae520a5864602ad5", "tenant70ead8d42e8111eba8f40a586461440e", "tenant6cae3f457c3d11ebbe1f0a586460451f",
    "tenant717bd6de35e4475cb62bcb110e14a6e0",
    "tenant0e9b44667b6211eb9ba10a58646140cf", "tenant99261e35447011ebad930a58646144b6", "tenant4d1ced39206111eb90940a5864600169",
    "tenantcbddccdb510c11ebb8aa0a5864613f91", "tenant4c44ab92206011eb9a740a5864604575", "tenant3c4667c0c492401f8d2a4018c3c5abca",
    "tenantf8eb4a6625c011eb981e0a5864606c4e", "tenant72edea2c54cf11ebb37a0a5864614457", "tenant927badb1879c41c7b3fe3edab2ed0f6c",
    "tenantc78bbd0a206d11ebbfc10a5864606c4f", "tenant1cf277fe2b5c11eb9c2a0a5864614384", "tenant9aa3846e28f811eb9eae0a5864606c52",
    "tenant48e7378f502011ebad930a58646144b6", "tenant7b8b63553f0911ebb85a0a5864606c7c", "tenant39584044c1f311eaa9a20a5864613f77",
    "tenantb8a332cd96b3498a94be60caa90f81e9", "tenant2daecc4d139f11eb9ca40a5864606c4f", "tenant88889a28b7a211eab6fd0a58646001d7",
    "tenantbce4c1bf7dd011eb90820a5864613db8"]

process_list = [
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

staging_list = [
        'sf1_consulta', 'sf1_invoicebra', 'sf2_consulta', 'sf2_invoicebra',
        'sd1_consulta', 'sd1_dados', 'sd1_devolution',
        'sd1_else', 'se1_installments', 'se1_invoice', 'se1_payments',
        'se1_payments_abatimentos', 'se2_installments',  'se2_invoice',
        'se2_payments', 'se2_payments_abatimentos',
        'cvd_else', 'cvd_contas_avaliadas',
    ]

params = dict(
    organization='totvstechfin',
    app_name="techfinplatform",
    connector_name='protheus_carol',
    connector_group='protheus',
    app_version='0.2.7',
    staging_list=staging_list,
    process_list=process_list,

)
