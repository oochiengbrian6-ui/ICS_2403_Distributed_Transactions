import json
import os
from datetime import datetime

class RecoveryLogger:
    def __init__(self, log_file='logs/transaction_log.json'):
        self.log_file = log_file
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
    def log_prepare(self, txn_id, from_acc, to_acc, amount):
        """Log transaction prepare phase."""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'txn_id': txn_id,
            'state': 'PREPARED',
            'from_account': from_acc,
            'to_account': to_acc,
            'amount': amount
        }
        self._append_log(entry)
    
    def log_commit(self, txn_id):
        """Log transaction commit."""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'txn_id': txn_id,
            'state': 'COMMITTED'
        }
        self._append_log(entry)
    
    def log_abort(self, txn_id):
        """Log transaction abort."""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'txn_id': txn_id,
            'state': 'ABORTED'
        }
        self._append_log(entry)
    
    def _append_log(self, entry):
        """Append entry to log file."""
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')
    
    def recover(self):
        """Recover system state from log after crash."""
        if not os.path.exists(self.log_file):
            return []
        
        with open(self.log_file, 'r') as f:
            lines = f.readlines()
        
        pending_transactions = []
        for line in lines:
            entry = json.loads(line)
            if entry['state'] == 'PREPARED':
                # Transaction was prepared but not committed/aborted
                pending_transactions.append(entry)
        
        return pending_transactions
