import socket
import json
import uuid
import time
from recovery_logger import RecoveryLogger

class TransactionManager:
    def __init__(self, participants):
        self.participants = participants
        self.recovery_logger = RecoveryLogger()
        
    def execute_transfer(self, from_account, to_account, amount):
        """Execute a transfer using Two-Phase Commit."""
        txn_id = str(uuid.uuid4())
        
        # Log transaction start
        self.recovery_logger.log_prepare(txn_id, from_account, to_account, amount)
        
        # PHASE 1: Prepare
        votes = []
        for participant in self.participants:
            try:
                # Determine operation type for each participant
                if participant['port'] == 5001:  # Node with account A
                    operation = 'WITHDRAW'
                    account = from_account
                else:  # Node with account B
                    operation = 'DEPOSIT'
                    account = to_account
                
                # Send prepare request
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((participant['host'], participant['port']))
                
                prepare_msg = {
                    'action': 'PREPARE',
                    'txn_id': txn_id,
                    'account': account,
                    'operation': operation,
                    'amount': amount
                }
                sock.send(json.dumps(prepare_msg).encode())
                
                # Receive vote
                response = sock.recv(1024).decode()
                vote = json.loads(response)
                votes.append(vote['status'] == 'YES')
                sock.close()
                
            except Exception as e:
                print(f"Error contacting participant: {e}")
                votes.append(False)
        
        # PHASE 2: Commit or Abort
        if all(votes):
            # All participants voted YES → Commit
            decision = 'COMMIT'
            for participant in self.participants:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((participant['host'], participant['port']))
                    
                    commit_msg = {'action': 'COMMIT', 'txn_id': txn_id}
                    sock.send(json.dumps(commit_msg).encode())
                    sock.close()
                except:
                    # Log failure but continue (recovery will handle it)
                    pass
            self.recovery_logger.log_commit(txn_id)
            return True
        else:
            # At least one NO → Abort
            decision = 'ABORT'
            for participant in self.participants:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((participant['host'], participant['port']))
                    
                    abort_msg = {'action': 'ABORT', 'txn_id': txn_id}
                    sock.send(json.dumps(abort_msg).encode())
                    sock.close()
                except:
                    pass
            self.recovery_logger.log_abort(txn_id)
            return False
