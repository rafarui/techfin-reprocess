from app.commons import params, tenant_list
from app.tasks import runPipeline
import luigi
from dotenv import load_dotenv
load_dotenv('.env', override=True)


if __name__ == "__main__":

    task_list = [runPipeline(tenant=tenant, **params) for tenant in tenant_list]

    exec_status = luigi.build(
        task_list, local_scheduler=False,
        workers=5, scheduler_port=8880,
        detailed_summary=True
        )
