from app import commons
from functions import carol_apps, carol_task, carol_login
import luigi
import logging
import time
import random

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)


class getLogin(commons.Task):

    tenant = luigi.Parameter()
    organization = luigi.Parameter()
    app_name = luigi.Parameter()

    def easy_run(self, inputs):
        out = carol_login.get_login(
            self.tenant, self.organization, self.app_name)
        return out


@commons.inherit_list(
    getLogin
)
class updateCarolApp(commons.Task):

    resources = {'app_install': 1}
    # connector_group = luigi.Parameter()

    def easy_run(self, inputs):
        login = inputs[0]
        current_version = carol_apps.get_app_version(login, self.app_name)
        for i in range(60):
            time.sleep(random.random())
            logger.debug(f'updating from {current_version} to {self.app_version}')
        return 'done'


##################################################################################################################

@commons.inherit_list(
    getLogin,
    updateCarolApp
)
class dropStaging(commons.Task):

    staging_list = luigi.ListParameter()

    def easy_run(self, inputs):
        login, _ = inputs
        for i in self.staging_list:
            time.sleep(random.random()*3)
            logger.debug(f'droping {i}')
        return 'done'


@commons.inherit_list(
    getLogin,
    dropStaging
)
class processTables(commons.Task):

    resources = {'process': 1}

    process_list = luigi.ListParameter()

    def easy_run(self, inputs):
        login, _ = inputs
        
        for i in self.process_list:
            time.sleep(random.random()*3)
            logger.debug(f'processin {i}')
        return 'done'


@commons.inherit_list(
    processTables
)
class runPipeline(commons.Task):

    tenant = luigi.Parameter()
    organization = luigi.Parameter()
    app_name = luigi.Parameter()
    process_list = luigi.ListParameter()
    staging_list = luigi.ListParameter()


    def easy_run(self, inputs):
        return "Done"