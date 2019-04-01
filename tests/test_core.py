import unittest
from unittest.mock import patch, Mock

from gce_task_runner import notify_completion, run, Task, Parameter


class NotifyCompletionTestCase(unittest.TestCase):

    @patch('gce_task_runner.pubsub.PublishClient')
    @patch('gce_task_runner.core._get_metadata')
    @patch('gce_task_runner.core._get_project')
    def test_notify_completion(self, _mock_get_project, _mock_get_metadata, _mock_publisher):
        expected_project = 'project'
        expected_topic = 'topic'
        expected_instance = 'xxx_id'
        _mock_get_project.side_effect = (expected_project,)
        _mock_get_metadata.side_effect = (expected_topic, expected_instance)
        publisher = Mock(return_value='')
        _mock_publisher.return_value = publisher
        notify_completion()
        _mock_publisher.assert_called_once_with(expected_project)
        publisher.publish.assert_called_once_with(expected_topic, expected_instance)

    @patch('gce_task_runner.pubsub.PublishClient')
    @patch('gce_task_runner.core._get_metadata')
    @patch('gce_task_runner.core._get_project')
    def test_notify_completion_error(self, _mock_get_project, _mock_get_metadata, _mock_publisher):
        expected_project = 'project'
        expected_topic = 'topic'
        expected_instance = 'xxx_id'
        expected_error = 'Error'
        _mock_get_project.side_effect = (expected_project,)
        _mock_get_metadata.side_effect = (expected_topic, expected_instance)
        publisher = Mock(return_value='')
        _mock_publisher.return_value = publisher
        notify_completion(error=expected_error)
        _mock_publisher.assert_called_once_with(expected_project)
        publisher.publish.assert_called_once_with(
            expected_topic, expected_instance, error=expected_error)

    @patch('gce_task_runner.pubsub.PublishClient')
    @patch('gce_task_runner.core._get_metadata')
    @patch('gce_task_runner.core._get_project')
    def test_notify_completion_no_id(self, _mock_get_project, _mock_get_metadata, _mock_publisher):
        expected_project = 'project'
        expected_topic = 'topic'
        expected_instance = None
        _mock_get_project.side_effect = (expected_project,)
        _mock_get_metadata.side_effect = (expected_topic, expected_instance)
        publisher = Mock(return_value='')
        _mock_publisher.return_value = publisher
        notify_completion()
        # インスタンスIDがなければ実行しない
        publisher.publish.assert_not_called()

    @patch('gce_task_runner.pubsub.PublishClient')
    @patch('gce_task_runner.core._get_metadata')
    @patch('gce_task_runner.core._get_project')
    def test_notify_completion_no_id(self, _mock_get_project, _mock_get_metadata, _mock_publisher):
        expected_project = 'project'
        expected_topic = None
        expected_instance = 'xxx_id'
        _mock_get_project.side_effect = (expected_project,)
        _mock_get_metadata.side_effect = (expected_topic, expected_instance)
        publisher = Mock(return_value='')
        _mock_publisher.return_value = publisher
        notify_completion()
        # トピックがなければ実行しない
        publisher.publish.assert_not_called()


class RunTestCase(unittest.TestCase):
    @patch('gce_task_runner.core._run_task')
    def test_run(self, _mock_run_task):
        _mock_run_task.return_value = []
        tasks = [
            Task('name', 'project', Parameter(
                instance_name='instance_name',
                startup_script='''
                #!/bin/bash
                shutdown -h now
                '''
            ))
        ]
        actual = run(tasks)
        self.assertEqual(None, actual)

    @patch('gce_task_runner.core._run_task')
    def test_run_error(self, _mock_run_task):
        _mock_run_task.side_effect = ([], ['Error'], [])
        tasks = [
            Task('task1', 'project', Parameter(
                instance_name='instance_name',
                startup_script='''
                #!/bin/bash
                shutdown -h now
                '''
            )),
            Task('task2', 'project', Parameter(
                instance_name='instance_name',
                startup_script='''
                #!/bin/bash
                shutdown -h now
                '''
            )),
            Task('task3', 'project', Parameter(
                instance_name='instance_name',
                startup_script='''
                #!/bin/bash
                shutdown -h now
                '''
            )),
        ]
        # エラーが発生した時点で終了
        actual = run(tasks)
        self.assertEqual(('task2', ['Error']), actual)
