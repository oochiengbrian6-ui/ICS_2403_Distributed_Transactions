import unittest
import subprocess
import time
import sqlite3

class TestDistributedTransactions(unittest.TestCase):
    
    def setUp(self):
        """Start the system before each test."""
        self.coordinator = subprocess.Popen(['python', 'src/coordinator.py'])
        self.participant1 = subprocess.Popen(['python', 'src/participant.py', '1'])
        self.participant2 = subprocess.Popen(['python', 'src/participant.py', '2'])
        time.sleep(3)  # Wait for nodes to start
    
    def tearDown(self):
        """Cleanup after each test."""
        self.coordinator.terminate()
        self.participant1.terminate()
        self.participant2.terminate()
    
    def test_atomic_transfer(self):
        """Test that transfers are atomic."""
        result = subprocess.run(
            ['python', 'src/client.py'],
            capture_output=True,
            text=True
        )
        self.assertIn('COMMITTED', result.stdout)
    
    def test_concurrent_access(self):
        """Test multiple concurrent clients."""
        from src.client import run_concurrent_clients
        try:
            run_concurrent_clients(3)
            self.assertTrue(True)  # No deadlock occurred
        except Exception as e:
            self.fail(f"Concurrent test failed: {e}")
    
    def test_balance_consistency(self):
        """Test that total balance is preserved."""
        conn1 = sqlite3.connect('bank_node_1.db')
        conn2 = sqlite3.connect('bank_node_2.db')
        
        cur1 = conn1.cursor()
        cur2 = conn2.cursor()
        
        cur1.execute("SELECT SUM(balance) FROM accounts")
        cur2.execute("SELECT SUM(balance) FROM accounts")
        
        total_before = cur1.fetchone()[0] + cur2.fetchone()[0]
        
        # Execute a transfer
        subprocess.run(['python', 'demo/single_transfer.py'], capture_output=True)
        
        cur1.execute("SELECT SUM(balance) FROM accounts")
        cur2.execute("SELECT SUM(balance) FROM accounts")
        
        total_after = cur1.fetchone()[0] + cur2.fetchone()[0]
        
        self.assertEqual(total_before, total_after, "Total balance should be preserved")
        
        conn1.close()
        conn2.close()

if __name__ == '__main__':
    unittest.main()
