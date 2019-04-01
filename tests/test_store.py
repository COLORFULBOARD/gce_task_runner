import unittest
from importlib import reload

from gce_task_runner import store


class GetRemainsCountTestCase(unittest.TestCase):
    def setUp(self):
        reload(store)

    def test_get_remains_count(self):
        expected = 3
        store.initialize(expected)
        self.assertEqual(expected, store.get_remains_count())

    def test_get_remains_count_uninitialized(self):
        with self.assertRaises(RuntimeError):
            store.get_remains_count()


class RegisterAndPopTestCase(unittest.TestCase):
    def setUp(self):
        reload(store)

    def test_register_uninitialized(self):
        with self.assertRaises(RuntimeError):
            store.register('xxx', object())

    def test_pop_uninitialized(self):
        with self.assertRaises(RuntimeError):
            store.pop('xxx')

    def test_pop_emtpy(self):
        store.initialize(1)
        self.assertEqual((None, None), store.pop('xxx'))

    def test_register(self):
        store.initialize(1)
        expected_id = 'xxx'
        expected_obj = object()
        store.register(expected_id, expected_obj)
        self.assertEqual(1, store.get_remains_count())

        actual = store.pop(expected_id)
        self.assertEqual(expected_obj, actual[0])
        self.assertIsNone(actual[1])
        self.assertEqual(0, store.get_remains_count())

    def test_register_timeout(self):
        store.initialize(1)
        expected_id = 'xxx'
        expected_obj = object()
        store.register(expected_id, expected_obj, timeout=30)
        actual = store.pop(expected_id)
        self.assertEqual(expected_obj, actual[0])
        self.assertEqual(float, type(actual[1]))


class GetTimeOversTestCase(unittest.TestCase):
    def setUp(self):
        reload(store)

    def test_get_time_overs(self):
        # 3台追加して2台タイムオーバー
        store.initialize(3)
        expected_id_0 = 'xxx'
        expected_obj_0 = object()
        store.register(expected_id_0, expected_obj_0, timeout=0.1)
        expected_id_1 = 'xxxx'
        expected_obj_1 = object()
        store.register(expected_id_1, expected_obj_1, timeout=1)
        expected_id_2 = 'xxxxx'
        expected_obj_2 = object()
        store.register(expected_id_2, expected_obj_2, timeout=0.1)

        # タイムオーバーになるまで待機
        import time
        time.sleep(0.2)
        actual = store.get_time_overs()
        self.assertEqual(2, len(actual))
