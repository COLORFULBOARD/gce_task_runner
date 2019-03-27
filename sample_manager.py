from gce_task_runner import Parameter, Task, run

PROJECT_ID = 'suyama-sandbox'


def main():
    tasks = (
        Task(
            name='task 1',
            project=PROJECT_ID,
            parameter=Parameter(
                instance_name='instance-1-{}',
                startup_script="""
                #! /bin/bash
                echo '##################### task1 ############################'
                apt update -y && apt upgrade -y && apt install python3-pip -y

                # Must call notify_completion()
                pip3 install git+https://github.com/COLORFULBOARD/gce_task_runner.git@dev#egg=gce-task-runner
                python3 -c 'from gce_task_runner import notify_completion; notify_completion()'
                """,
                instances=3,
            )
        ),
        Task(
            name='task 2',
            project=PROJECT_ID,
            parameter=Parameter(
                instance_name='instance-2',
                startup_script="""
                #! /bin/bash
                echo '##################### task2 ############################'
                apt update -y && apt upgrade -y && apt install python3-pip -y

                # Must call notify_completion()
                pip3 install git+https://github.com/COLORFULBOARD/gce_task_runner.git@dev#egg=gce-task-runner
                python3 -c 'from gce_task_runner import notify_completion; notify_completion()'
                """,
            ),
            timeout=30,
        ),
    )
    run(tasks)


if __name__ == '__main__':
    main()
