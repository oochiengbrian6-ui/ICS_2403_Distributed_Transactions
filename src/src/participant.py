import socket
import threading
import json
import sqlite3
from lock_manager import LockManager

class Participant:
    def __init__(self, node_id, host='localhost', port=5001):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.lock_manager = LockManager()
        self.setup_database()
        
    def setup_database(self):
        """Initialize SQLite database with accounts."""
        self.conn = sqlite3.connect(f'bank_node_{self.node_id}.db')
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS accounts
                         (account_id TEXT PRIMARY KEY, balance REAL)''')
        # Seed with initial data
        if self.node_id == 1:
            cursor.execute("INSERT OR REPLACE INTO accounts VALUES ('A', 1000.0)")
        else:
            cursor.execute("INSERT OR REPLACE INTO accounts VALUES ('B', 500.0)")
        self.conn.commit()
    
    def handle_coordinator(self, client_socket, addr):
        """Handle messages from coordinator."""
        try:
            data = client_socket.recv(1024).decode()
            message = json.loads(data)
            
            if message['action'] == 'PREPARE':
                # Acquire lock and check if operation can proceed
                account = message['account']
                if self.lock_manager.acquire_lock(account, message['txn_id']):
                    response = {'status': 'YES', 'txn_id': message['txn_id']}
                else:
                    response = {'status': 'NO', 'txn_id': message['txn_id']}
            
            elif message['action'] == 'COMMIT':
                # Execute the transaction
                self.execute_operation(message)
                self.lock_manager.release_lock(message['account'], message['txn_id'])
                response = {'status': 'ACK'}
                
            elif message['action'] == 'ABORT':
                # Release lock without changes
                self.lock_manager.release_lock(message['account'], message['txn_id'])
                response = {'status': 'ACK'}
                
            client_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Participant error: {e}")
            response = {'status': 'ERROR'}
        finally:
            client_socket.close()
    
    def execute_operation(self, message):
        """Execute the actual database operation."""
        cursor = self.conn.cursor()
        if message['operation'] == 'WITHDRAW':
            cursor.execute("UPDATE accounts SET balance = balance - ? WHERE account_id = ?",
                          (message['amount'], message['account']))
        elif message['operation'] == 'DEPOSIT':
            cursor.execute("UPDATE accounts SET balance = balance + ? WHERE account_id = ?",
                          (message['amount'], message['account']))
        self.conn.commit()
    
    def start(self):
        """Start participant server."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Participant {self.node_id} listening on {self.host}:{self.port}")
        
        while True:
            client_socket, addr = server.accept()
            thread = threading.Thread(target=self.handle_coordinator, args=(client_socket, addr))
            thread.start()

if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    port = 5000 + node_id
    participant = Participant(node_id, port=port)
    participant.start()
