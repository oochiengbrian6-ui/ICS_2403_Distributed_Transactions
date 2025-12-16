import socket
import threading
import json
import time
from transaction_manager import TransactionManager

class Coordinator:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.participants = [
            {'host': 'localhost', 'port': 5001},
            {'host': 'localhost', 'port': 5002}
        ]
        self.transaction_manager = TransactionManager(self.participants)
        self.running = True
        
    def handle_client(self, client_socket, addr):
        """Handle incoming transaction requests from clients."""
        try:
            data = client_socket.recv(1024).decode()
            request = json.loads(data)
            
            if request['type'] == 'TRANSFER':
                result = self.transaction_manager.execute_transfer(
                    request['from_account'],
                    request['to_account'],
                    request['amount']
                )
                response = {'status': 'COMMITTED' if result else 'ABORTED'}
            else:
                response = {'status': 'ERROR', 'message': 'Invalid request'}
                
            client_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()
    
    def start(self):
        """Start coordinator server."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Coordinator listening on {self.host}:{self.port}")
        
        while self.running:
            client_socket, addr = server.accept()
            thread = threading.Thread(target=self.handle_client, args=(client_socket, addr))
            thread.start()

if __name__ == "__main__":
    coordinator = Coordinator()
    coordinator.start()
