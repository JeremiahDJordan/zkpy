#! /bin/env python
import unittest
import zkpy.exceptions
import zkpy.connection
import zkpy.lock
import zkpy.acl
import time
import logging
import inspect
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ZOOKEEPER_HOST='localhost:2181'

class TestLock(unittest.TestCase):
    def testLockSynchronous(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode)
                self.assertEqual(lock1.acquire(), True)
                self.assertNotEqual(lock1.id, '')
                self.assertEqual(lock1.path,lockNode)
                self.assertEqual(lock1.release(), None)
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testLockNoAcquireNodeException(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode)
                conn1.delete(lockNode)
                self.assertRaises(zkpy.exceptions.NoNodeException, lock1.acquire)
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testLockSynchronousTwice(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode)
                self.assertEqual(lock1.acquire(), True)
                self.assertEqual(lock1.acquire(), True)
                self.assertNotEqual(lock1.id, '')
                self.assertEqual(lock1.path,lockNode)
                self.assertEqual(lock1.release(), None)
                self.assertEqual(lock1.release(), None)
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testLockSynchronousConnectionClosed(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            lockNode = '/locktest_'+inspect.stack()[0][3]
            conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
            lock1 = zkpy.lock.Lock(conn1, lockNode)
            self.assertEqual(lock1.acquire(), True)
            conn1.close()
            self.assertEqual(lock1.release(), None)

    def testLockNoNodeException(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                try:
                    conn1.delete(lockNode)
                except:
                    pass
                self.assertRaises(zkpy.exceptions.NoNodeException, zkpy.lock.Lock, conn1, lockNode)
            finally:
                pass

    def testLockRunTimeException(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                def FakeCreate(self, path, *args, **_kwargs):
                    return path+"/a6d255ad-6ab4-49f7-9f92-d5b4ad703af7"
                conn1.create = FakeCreate
                lock1 = zkpy.lock.Lock(conn1, lockNode)
                self.assertRaises(RuntimeError,lock1.acquire)
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testLockSynchronousCreate2(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode)
                lock2 = zkpy.lock.Lock(conn1, lockNode)
                self.assertNotEqual(lock1.id, '')
                self.assertEqual(lock1.path,lockNode)
                self.assertEqual(lock2.acquire(), True)
                self.assertNotEqual(lock1.id, lock2.id)
                self.assertEqual(lock1.path,lock2.path)
                self.assertEqual(lock1.release(), None)
                self.assertEqual(lock2.release(), None)
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testLockAsynchronous(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            class LockObserver(object):
                def __init__(self):
                    self.acquired = None
                    self.released = None
                def lock_acquired(self):
                    self.acquired = True
                def lock_released(self):
                    self.released = True
            observer = LockObserver()
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode, observer)
                self.assertEqual(lock1.acquire(), True)
                self.assertEqual((observer.acquired, observer.released), (True, None))
                self.assertEqual(lock1.release(), None)
                self.assertEqual((observer.acquired, observer.released), (True, True))
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testMultipleLockAsynchronous(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            class LockObserver(object):
                def __init__(self):
                    self.acquired = None
                    self.released = None
                def lock_acquired(self):
                    self.acquired = True
                def lock_released(self):
                    self.released = True
            observer1 = LockObserver()
            observer2 = LockObserver()
            #observer3 = LockObserver()
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode, observer1)
                self.assertEqual(lock1.acquire(), True)
                self.assertEqual((observer1.acquired, observer1.released), (True, None))
                with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
                    lock2 = zkpy.lock.Lock(conn1, lockNode, observer2)
                    self.assertEqual(lock2.acquire(), False)
                    self.assertEqual((observer2.acquired, observer2.released), (None, None))
                    self.assertEqual(lock1.release(), None)
                    self.assertEqual((observer1.acquired, observer1.released), (True, True))
                    time.sleep(0.1)
                    self.assertEqual((observer2.acquired, observer2.released), (True, None))
                    self.assertEqual(lock2.release(), None)
                    self.assertEqual((observer2.acquired, observer2.released), (True, True))
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass

    def testMultipleLockAsynchronousDel(self):
        with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
            class LockObserver(object):
                def __init__(self):
                    self.acquired = None
                    self.released = None
                def lock_acquired(self):
                    self.acquired = True
                def lock_released(self):
                    self.released = True
            observer1 = LockObserver()
            observer2 = LockObserver()
            try:
                lockNode = '/locktest_'+inspect.stack()[0][3]
                conn1.ensure_path_exists(lockNode, '', [zkpy.acl.Acls.Unsafe])
                lock1 = zkpy.lock.Lock(conn1, lockNode, observer1)
                self.assertEqual(lock1.acquire(), True)
                self.assertEqual((observer1.acquired, observer1.released), (True, None))
                with zkpy.connection.zkopen(ZOOKEEPER_HOST, 5) as conn1:
                    lock2 = zkpy.lock.Lock(conn1, lockNode, observer2)
                    self.assertEqual(lock2.acquire(), False)
                    self.assertEqual((observer2.acquired, observer2.released), (None, None))
                    self.assertEqual(lock1.release(), None)
                    self.assertEqual((observer1.acquired, observer1.released), (True, True))
                    time.sleep(0.1)
                    self.assertEqual((observer2.acquired, observer2.released), (True, None))
                    lock2.__del__()
                    time.sleep(0.1)
                    self.assertEqual((observer2.acquired, observer2.released), (True, True))
            finally:
                try:
                    conn1.delete(lockNode)
                except:
                    pass


if __name__ == '__main__':
    unittest.main()

