import logging
import threading
import time
import uuid
from functools import partial

import requests
from asynconsumer import async_run

from . import gce, pubsub

logging.captureWarnings(True)
logger = logging.getLogger(__name__)

_LOCK = threading.Lock()

# 稼働中GCEインスタンスの辞書。{_id: (instance, タイムリミット)}
_INSTANCES = {}

# 未終了GCEインスタンス数
_INSTANCE_SIZE = 0

# GCEインスタンスから通知されたエラー
_ERRORS = []


class Task:
    """タスククラス."""

    def __init__(self,
                 name,
                 project,
                 parameter,
                 timeout=0,
                 retry_quota_exceeded=False):  # noqa: D107
        self.name = name
        self.project = project
        self.parameter = parameter
        self.timeout = timeout
        self.retry_quota_exceeded = retry_quota_exceeded


class Parameter:
    """インスタンスパラメータクラス."""

    def __init__(self,
                 instance_name,
                 startup_script=None,
                 shutdown_script=None,
                 startup_script_url=None,
                 shutdown_script_url=None,
                 instances=1,
                 image='projects/ubuntu-os-cloud/global/images/ubuntu-1804-bionic-v20190320',
                 machine_type='n1-standard-1',
                 zone='asia-northeast1-b',
                 disk_size=20,
                 metas=None,
                 gpu_info=None,
                 minCpuPlatform=None,
                 preemptible=False):  # noqa: D107

        if len(list(filter(lambda x: bool(x), (startup_script, startup_script_url)))) != 1:
            raise ValueError('Set only one of startup_script and startup_script_url')

        self.instance_name = instance_name
        self.startup_script = startup_script
        self.startup_script_url = startup_script_url
        self.shutdown_script = shutdown_script
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


def notify_completion(project=None, topic=None, error=None):
    """タスクの完了を通知する."""
    try:
        project = project or _get_project()
        topic = topic or _get_metadata('topic')
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
                logger.info('notify_completion is not completed: {}'.format(e))
        else:
            logger.info('notify_completion is not sent.')
    except Exception:
        # finally節で実行されることを想定
        pass


def run(tasks, topic='manager', subscription='manager', project=None):
    """タスクリストを実行する."""
    if topic != 'manager' or subscription != 'manager' or project:
        # TODO: 2.0.0でtask以外の引数を消す
        import warnings
        warnings.warn(("Optional args (topic, subscription, project) are deprecated."
                       " These do not work now."
                       ), DeprecationWarning)

    for task in tasks:
        error = _run_task(task)
        if error:
            return task.name, error


def _add_instance(_id, instance, timeout=0):
    """_INSTANCESにGCEインスタンスを格納する"""
    with _LOCK:
        limit = timeout + time.time() if timeout else None
        _INSTANCES[_id] = (instance, limit)


def _pop_instance(_id):
    """_INSTANCESに格納されたGCEインスタンスを取り出す"""
    with _LOCK:
        return _INSTANCES.pop(_id, (None, None))


def _get_time_overs():
    """_INSTANCESに格納された期限切れGCEインスタンスを全て取り出す"""
    with _LOCK:
        ids = [_id for _id, (_, limit) in _INSTANCES.items() if limit and limit < time.time()]
        return [(_id, _INSTANCES.pop(_id)) for _id in ids]


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


def _run_task(task):
    """個別のタスクを実行する"""
    logger.info('start to {}'.format(task.name))

    global _INSTANCE_SIZE
    _INSTANCE_SIZE = task.parameter.instances

    # インスタンス作成中でも完了通知を受信できるようにしておく
    topic = _subscribe_in_background(task)

    # 100台ずつ並列でインスタンスの作成
    async_run(range(task.parameter.instances), partial(_create_instance, task, topic),
              concurrency=100, sleep=0)

    # 全台処理が終了するまで待機
    while _INSTANCE_SIZE > 0:
        time.sleep(5)

    logger.info('finish to {}'.format(task.name))
    return _ERRORS


def _subscribe_in_background(task):
    """バックグラウンドスレッドでGCEインスタンスからの完了通知を受け取る"""
    project = task.project
    topic = f"task-{task.name.replace(' ', '-')}-{str(uuid.uuid4())}"
    subscription = f"task-{task.name.replace(' ', '-')}"

    def callback(message):
        # インスタンスからの完了通知を受け取った時の処理
        global _INSTANCE_SIZE
        instance_id = message.data.decode('utf-8')
        instance, _ = _pop_instance(instance_id)
        if instance:
            if 'error' in message.attributes:
                # errorメッセージが含まれていたらエラーとして処理する、それ以外は正常終了扱い
                error_msg = message.attributes['error']
                logger.info(
                    'Error occurred while executing the task({}) in {}: {}'.format(
                        task.name, instance_id, error_msg))
                _ERRORS.append(f'{error_msg} found in {instance_id}')
            else:
                logger.info('instance {} is completed'.format(instance_id))

            # インスタンスの削除
            instance.delete()
            logger.info('instance {} is terminated'.format(instance_id))
            _INSTANCE_SIZE -= 1

    def stop_callback():
        # Trueを返すとPubSubの監視を終了する
        global _INSTANCE_SIZE
        if task.timeout:
            # 時間切れのインスタンスを削除
            for _id, (instance, _) in _get_time_overs():
                logger.info('instance {} is timeout!!!'.format(_id))
                instance.delete()
                logger.info('instance {} is terminated'.format(_id))
                _INSTANCE_SIZE -= 1
        return _INSTANCE_SIZE == 0

    thread = threading.Thread(target=_subscribe,
                              args=(project, topic, subscription, callback, stop_callback))
    thread.start()
    return topic


def _subscribe(project, topic, subscription, callback, stop_callback):
    """サブスクライブの実行"""
    with pubsub.context(project, topic, subscription) as subscriber:
        subscriber.subscribe(subscription, callback, stop_callback)


def _create_instance(task, topic, num):
    """GCEインスタンスを作成する

    作成されたインスタンスは _INSTANCES に追加される
    task.retry_quota_exceeded がTrueの場合はQUOTAエラー時はリトライする
    :param task: タスク
    :param topic: GCEインスタンスが完了通知を飛ばすトピック
    :param num: タスク内でのそのインスタンスの通し番号
    """
    param = task.parameter
    _id = str(uuid.uuid4())
    metas = [
                {'key': 'instance-id', 'value': _id},
                {'key': 'instance-number', 'value': num},
                {'key': 'topic', 'value': topic},
            ] + param.metas[num]
    # googleapiclientがImportErrorをたくさん出すので抑制
    logging.disable(logging.FATAL)
    instance = gce.Client(
        param.instance_name.format(num),
        param.startup_script,
        param.startup_script_url,
        param.shutdown_script,
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
    while True:
        try:
            instance.create()
            logger.info(f'{param.instance_name.format(num)}({_id}) is created')
        except Exception as e:
            if task.retry_quota_exceeded and 'Quota' in str(e) and 'exceeded' in str(e):
                # リトライする
                logger.debug('Retry because quota exceeded')
                time.sleep(30)
                continue
            else:
                # リトライ不要であればエラーにして終了
                raise
        else:
            _add_instance(_id, instance, task.timeout)
            break
