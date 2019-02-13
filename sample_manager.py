from gce_task_runner import Parameter, Task, run

GCP = ''
TOPIC = ''
SUBSCRIPTION = ''

# 起動スクリプトではnotify_completion()を呼び出し完了をmanagerに伝える
STARTUP_SCRIPT_URL_TASK_1 = 'gs://...'
STARTUP_SCRIPT_URL_TASK_2 = 'gs://...'


def main():
    tasks = (
        Task(
            name='task 1',
            project=GCP,
            parameter=Parameter(
                instance_name='instance 1-{}',
                startup_script_url=STARTUP_SCRIPT_URL_TASK_1,
                instances=2,
            )
        ),
        Task(
            name='task 2',
            project=GCP,
            parameter=Parameter(
                instance_name='instance 2',
                startup_script_url=STARTUP_SCRIPT_URL_TASK_2,
            )
        ),
    )
    run(tasks, topic=TOPIC, subsctiption=SUBSCRIPTION)


if __name__ == '__main__':
    main()

