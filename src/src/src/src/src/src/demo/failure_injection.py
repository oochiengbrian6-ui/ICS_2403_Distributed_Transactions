import subprocess
import time
import signal
import os

def simulate_node_crash():
    """Simulate a participant node crash during transaction."""
    print("Starting normal transaction processing...")
    
    # Start all nodes
    coordinator_proc = subprocess.Popen(['python', 'src/coordinator.py'])
    participant1_proc = subprocess.Popen(['python', 'src/participant.py', '1'])
    participant2_proc = subprocess.Popen(['python', 'src/participant.py', '2'])
    
    time.sleep(2)  # Let nodes start
    
    print("\n1. Starting concurrent transactions...")
    subprocess.run(['python', 'src/client.py'])
    
    print("\n2. Simulating Participant 1 crash in 3 seconds...")
    time.sleep(3)
    
    # Crash participant 1
    participant1_proc.terminate()
    print("✓ Participant 1 CRASHED!")
    
    print("\n3. Attempting transaction with crashed node...")
    # This should fail or timeout
    subprocess.run(['python', 'demo/test_failure.py'])
    
    print("\n4. Restarting Participant 1 for recovery...")
    participant1_proc = subprocess.Popen(['python', 'src/participant.py', '1'])
    time.sleep(2)
    
    print("\n5. Testing recovery with new transaction...")
    subprocess.run(['python', 'src/client.py'])
    
    # Cleanup
    coordinator_proc.terminate()
    participant1_proc.terminate()
    participant2_proc.terminate()
    
    print("\n✓ Failure simulation complete!")

if __name__ == "__main__":
    simulate_node_crash()
