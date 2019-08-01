import logging
import time
from enum import Enum

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GPU(Enum):
    """GPUタイプのEnum."""

    V100 = 'nvidia-tesla-v100'
    K80 = 'nvidia-tesla-k80'
    P100 = 'nvidia-tesla-p100'


class Client:
    """GCEのAPI用のクライアントクラス."""

    def __init__(self,
                 instance,
                 startup_script,
                 startup_script_url,
                 shutdown_script,
                 shutdown_script_url,
                 project,
                 zone,
                 machine_type,
                 image,
                 disk_size,
                 metas,
                 gpu_info,
                 minCpuPlatform,
                 preemptible,
                 labels):  # noqa: D107
        self.service = build('compute', 'v1')
        # Required
        self.instance = instance
        self.startup_script = startup_script
        self.startup_script_url = startup_script_url
        self.shutdown_script = shutdown_script
        self.shutdown_script_url = shutdown_script_url
        self.project = project
        # Optional
        self.zone = zone
        self.machine_type = machine_type
        self.image = image
        self.disk_size = disk_size
        self.metas = metas
        self.gpu_info = gpu_info
        self.minCpuPlatform = minCpuPlatform
        self.preemptible = preemptible
        self.region = self.zone[:-2]
        self.labels = labels

    def create(self):
        """インスタンス作成."""
        operation = self.create_async()
        return self.wait_for_operation(operation['name'])

    def create_async(self):
        """インスタンス作成(非同期)."""
        try:
            return self.service.instances().insert(
                project=self.project,
                zone=self.zone,
                body=self.config,
            ).execute()
        except HttpError:
            raise

    def delete(self):
        """インスタンス削除."""
        try:
            operation = self.delete_async()
            return self.wait_for_operation(operation['name'])
        except Exception:
            return {'status': "DONE"}

    def delete_async(self):
        """インスタンス削除(非同期)."""
        try:
            return self.service.instances().delete(
                project=self.project,
                zone=self.zone,
                instance=self.instance
            ).execute()
        except HttpError as e:
            if 'HttpError 404' in str(e):
                logger.info("{} has been deleted".format(self.instance))
                # すでに存在しない場合は正常時と同じ型のダミーを返す
                return {'error': str(e)}
            logger.warning('error: {}'.format(e))
            raise

    @property
    def config(self):
        """APIパラメータ."""
        items = [
        ]
        if self.startup_script_url:
            items.append(
                {
                    "key": "startup-script-url",
                    "value": self.startup_script_url,
                }
            )
        elif self.startup_script:
            items.append(
                {
                    "key": "startup-script",
                    "value": self.startup_script,
                }
            )
        else:
            # 起動スクリプトは必ず必要
            raise ValueError('Set only one of startup_script and startup_script_url')

        if self.shutdown_script_url:
            items.append(
                {
                    "key": "shutdown-script-url",
                    "value": self.shutdown_script_url,
                }
            )
        elif self.shutdown_script:
            items.append(
                {
                    "key": "shutdown-script",
                    "value": self.shutdown_script,
                }
            )

        items.extend(self.metas)
        _config = {
            "name": self.instance,
            "zone": "projects/{}/zones/{}".format(self.project, self.zone),
            "machineType": "projects/{}/zones/{}/machineTypes/{}".format(self.project,
                                                                         self.zone,
                                                                         self.machine_type),
            "metadata": {
                "items": items,
            },
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initializeParams": {
                        "sourceImage": self.image,
                        "diskSizeGb": self.disk_size,
                    }
                }
            ],
            "networkInterfaces": [
                {
                    "subnetwork": "projects/{}/regions/{}/subnetworks/default".format(
                        self.project, self.region),
                    "accessConfigs": [
                        {
                            "kind": "compute#accessConfig",
                            "name": "External NAT",
                            "type": "ONE_TO_ONE_NAT",
                            "networkTier": "PREMIUM"
                        }
                    ],
                }
            ],
            "serviceAccounts": [
                {
                    "email": "default",
                    "scopes": [
                        "https://www.googleapis.com/auth/cloud-platform"
                    ]
                }
            ],
            "scheduling": {
                "automaticRestart": False,
                "onHostMaintenance": "TERMINATE" if self.preemptible else "MIGRATE",
                "preemptible": self.preemptible,
            }
        }
        if self.minCpuPlatform:
            _config['minCpuPlatform'] = self.minCpuPlatform
        if self.gpu_info:
            num, gpu = self.gpu_info
            _config["guestAccelerators"] = [
                {
                    "acceleratorCount": num,
                    "acceleratorType": "projects/{}/zones/{}/acceleratorTypes/{}".format(
                        self.project, self.zone, gpu.value)
                }
            ]
            _config["scheduling"]["onHostMaintenance"] = "TERMINATE"
        if self.labels:
            _config["labels"] = self.labels
        return _config

    def wait_for_operation(self, operation):
        """ジョブの待機. ポーリングによって実現."""
        logger.debug(f'Waiting for {operation} to finish...')
        while True:
            result = self.service.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=operation).execute()

            if result['status'] == 'DONE':
                logger.debug("done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result
            time.sleep(1)
