import threading
import time

class LockManager:
    def __init__(self):
        self.locks = {}  # account_id -> (txn_id, timestamp)
        self.lock = threading.Lock()
        
    def acquire_lock(self, account_id, txn_id):
        """Acquire exclusive lock for an account."""
        with self.lock:
            if account_id in self.locks:
                # Lock already held by another transaction
                return False
            else:
                self.locks[account_id] = (txn_id, time.time())
                return True
    
    def release_lock(self, account_id, txn_id):
        """Release lock for an account."""
        with self.lock:
            if account_id in self.locks and self.locks[account_id][0] == txn_id:
                del self.locks[account_id]
                return True
            return False
    
    def has_lock(self, account_id, txn_id):
        """Check if transaction holds lock."""
        with self.lock:
            return account_id in self.locks and self.locks[account_id][0] == txn_id
