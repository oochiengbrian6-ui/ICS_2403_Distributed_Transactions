import socket
import json
import threading
import time
import random

def send_transaction_request(from_acc, to_acc, amount, client_id):
    """Send a transaction request to coordinator."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 5000))
        
        request = {
            'type': 'TRANSFER',
            'from_account': from_acc,
            'to_account': to_acc,
            'amount': amount,
            'client_id': client_id
        }
        
        sock.send(json.dumps(request).encode())
        response = sock.recv(1024).decode()
        result = json.loads(response)
        
        print(f"Client {client_id}: Transfer ${amount} from {from_acc} to {to_acc} -> {result['status']}")
        sock.close()
    except Exception as e:
        print(f"Client {client_id} failed: {e}")

def run_concurrent_clients(num_clients=5):
    """Run multiple clients concurrently."""
    threads = []
    
    for i in range(num_clients):
        amount = random.uniform(10, 100)
        # Alternate between A→B and B→A transfers
        if i % 2 == 0:
            from_acc, to_acc = 'A', 'B'
        else:
            from_acc, to_acc = 'B', 'A'
            
        thread = threading.Thread(
            target=send_transaction_request,
            args=(from_acc, to_acc, amount, i)
        )
        threads.append(thread)
        thread.start()
        time.sleep(random.uniform(0.1, 0.5))  # Stagger requests
    
    for thread in threads:
        thread.join()
    
    print(f"\n{num_clients} concurrent transactions completed.")

if __name__ == "__main__":
    run_concurrent_clients(5)
