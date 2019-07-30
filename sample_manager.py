import logging

from gce_task_runner import Parameter, Task, run

PROJECT_ID = "<YOUR GCP PROJECT ID>"

root = logging.getLogger()
root.addHandler(logging.StreamHandler())
root.setLevel(logging.INFO)


def main():
    tasks = (
        Task(
            name="task 1",
            project=PROJECT_ID,
            parameter=Parameter(
                instance_name="instance-1-{}",
                startup_script="""
                #! /bin/bash
                echo '##################### task1 ############################'
                echo -e 'FROM python:3.7\nRUN pip install git+https://github.com/COLORFULBOARD/gce_task_runner#egg=gce-task-runner' > /tmp/Dockerfile
                docker build -f /tmp/Dockerfile -t worker:1.0 /tmp
                docker run --rm worker:1.0 python3 -c 'from gce_task_runner import notify_completion;notify_completion()'
                """,
                instances=3,
                image="projects/cos-cloud/global/images/cos-69-10895-299-0",
            ),
        ),
        Task(
            name="task 2",
            project=PROJECT_ID,
            parameter=Parameter(
                instance_name="instance-2",
                startup_script="""
                #! /bin/bash
                echo '##################### task2 ############################'
                echo -e 'FROM python:3.7\nRUN pip install git+https://github.com/COLORFULBOARD/gce_task_runner#egg=gce-task-runner' > /tmp/Dockerfile
                docker build -f /tmp/Dockerfile -t worker:1.0 /tmp
                docker run --rm worker:1.0 python3 -c 'from gce_task_runner import notify_completion;notify_completion()'
                """,
                image="projects/cos-cloud/global/images/cos-69-10895-299-0",
            ),
            timeout=30,
        ),
    )
    run(tasks)


if __name__ == "__main__":
    main()
