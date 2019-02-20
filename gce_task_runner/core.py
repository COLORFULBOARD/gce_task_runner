import logging
import time
import uuid

import requests

from . import gce, pubsub

logger = logging.getLogger(__name__)


class Task:
    """タスククラス."""

    def __init__(self,
                 name,
                 project,
                 parameter,
                 timeout=0):  # noqa: D107
        self.name = name
        self.project = project
        self.parameter = parameter
        self.timeout = timeout


class Parameter:
    """インスタンスパラメータクラス."""

    def __init__(self,
                 instance_name,
                 startup_script_url=None,
                 shutdown_script_url=None,
                 instances=1,
                 image='projects/debian-cloud/global/images/debian-9-stretch-v20180716',
                 machine_type='n1-standard-1',
                 zone='asia-northeast1',
                 disk_size=20,
                 metas=None,
                 gpu_info=None,
                 minCpuPlatform=None,
                 preemptible=False):  # noqa: D107
        self.instance_name = instance_name
        self.startup_script_url = startup_script_url
        self.shutdown_script_url = shutdown_script_url
        self.instances = instances
        self.image = image
        self.machine_type = machine_type
        self.zone = zone
        self.disk_size = disk_size
        self.metas = metas or [[{}] for _ in range(instances)]
        self.gpu_info = gpu_info
        self.minCpuPlatform = minCpuPlatform
        self.preemptible = preemptible


def _get_metadata(key):
    try:
        res = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/attributes/{}".format(key),
            headers={"Metadata-Flavor": "Google"})
    except Exception:
        pass
    else:
        if res.status_code == 200:
            return res.text
    return None


def _get_project():
    try:
        res = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"})
    except Exception:
        pass
    else:
        if res.status_code == 200:
            return res.text
    return None


def notify_completion(project=None, topic='manager', error=None):
    """タスクの完了を通知する."""
    try:
        project = project or _get_project()
        _id = _get_metadata('instance-id')
        if _id:
            publisher = pubsub.PublishClient(project)
            try:
                if error:
                    publisher.publish(topic, _id, error=str(error))
                else:
                    publisher.publish(topic, _id)
                logger.info('notify_completion: {}'.format(_id))
            except Exception as e:
                logger.info('notify_completion was not completed: {}'.format(e))
        else:
            logger.info('notify_completion is not sent.')
    except Exception:
        # finally節で実行されることを想定
        pass


def run(tasks, topic='manager', subscription='manager', project=None):
    """タスクを実行する."""
    if not project and tasks:
        project = tasks[0].project

    with pubsub.context(project, topic, subscription) as subscriber:
        for task in tasks:
            succeeded = _run_task(subscriber, task, subscription)
            if not succeeded:
                break


def _run_task(subscriber, task, subscription):
    logger.info('start to {}'.format(task.name))
    clients = _create_gce_clients(task)
    logger.info('created instances: {}'.format(','.join(clients)))

    if task.timeout:
        limit = task.timeout + time.time()
        logger.info(('set timer: instances will be terminated after {} sec'
                     ' even if the task will not be completed').format(task.timeout))

    is_error = False

    # インスタンスからの完了通知を受け取るまで待機
    def callback(message):
        instance_id = message.data.decode('utf-8')
        if instance_id in clients:
            # errorメッセージが含まれていたらエラーとして処理する、それ以外は正常終了扱い
            if 'error' in message.attributes:
                # エンクロージングスコープの変数に再代入するための宣言
                nonlocal is_error
                is_error = True
                error_msg = message.attributes['error']
                logger.info('Error occurred while executing the task({}): {}'.format(task.name, error_msg))

            # インスタンスの削除
            logger.info('instance {} is finished'.format(instance_id))
            clients[instance_id].delete()
            del clients[instance_id]
            logger.info('instance {} is terminated'.format(instance_id))

    def check():
        # Falseを返すとPubSubの監視を終了する
        if task.timeout and limit < time.time():
            logger.info('timeout!!!')
            # 全部消す
            for client in clients.values():
                client.delete()
            return False
        return len(clients)

    logger.info('waiting ...')
    subscriber.subscribe(subscription, callback, check)

    if is_error:
        # エラーが発生した場合は後続のTaskを実行せず終了する
        return False

    logger.info('finish to {}'.format(task.name))
    return True


def _create_gce_clients(task):
    param = task.parameter
    clients = {}

    # インスタンス作成の準備
    for i in range(param.instances):
        _id = str(uuid.uuid4())
        metas = [
            {'key': 'instance-id', 'value': _id},
            {'key': 'instance-number', 'value': i}
        ] + param.metas[i]
        # googleapiclientがImportErrorをたくさん出すので抑制
        logging.disable(logging.FATAL)
        clients[_id] = gce.Client(
            param.instance_name.format(i),
            param.startup_script_url,
            param.shutdown_script_url,
            task.project,
            zone=param.zone,
            machine_type=param.machine_type,
            image=param.image,
            disk_size=param.disk_size,
            metas=metas,
            gpu_info=param.gpu_info,
            minCpuPlatform=param.minCpuPlatform,
            preemptible=param.preemptible,
        )
        logging.disable(logging.NOTSET)

    # インスタンスの作成
    logger.info('create instances')
    for client in clients.values():
        client.create()

    return clients
