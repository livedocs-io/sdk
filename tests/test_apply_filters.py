import unittest
import polars as pl
import sys
import os

# Add the parent directory to sys.path to allow direct imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the function from the original module
from livedocs.utils.table_helpers import apply_filters

# Test class for apply_filters function
class TestApplyFilters(unittest.TestCase):
    def setUp(self):
        # Create a test DataFrame with various data types
        self.df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [95.5, 87.2, 76.8, 92.1, 88.5],
            "active": [True, False, True, True, False],
            "date": ["2023-01-01", "2023-02-15", "2023-03-20", "2023-04-10", "2023-05-05"]
        })
    
    def test_no_filters(self):
        """Test with empty filter conditions"""
        result = apply_filters(self.df, [])
        self.assertEqual(len(result), 5)  # Should return original DataFrame
    
    def test_single_filter(self):
        """Test with a single filter condition"""
        # Test equality filter
        result = apply_filters(self.df, [{"column": "name", "operator": "eq", "value": "Alice"}])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0, "name"], "Alice")
        
        # Test greater than filter
        result = apply_filters(self.df, [{"column": "age", "operator": "gt", "value": 30}])
        self.assertEqual(len(result), 3)  # 35, 40, 45
        
        # Test string contains filter
        result = apply_filters(self.df, [{"column": "name", "operator": "contains", "value": "a"}])
        self.assertEqual(len(result), 2)  # Charlie, David (note: 'a' is case-sensitive, so Alice is not included)
    
    def test_multiple_filters(self):
        """Test with multiple filter conditions"""
        # Test multiple filters (age > 30 AND active = True)
        result = apply_filters(self.df, [
            {"column": "age", "operator": "gt", "value": 30},
            {"column": "active", "operator": "eq", "value": True}
        ])
        self.assertEqual(len(result), 2)  # Only Charlie and David meet both conditions
        
        # Verify the correct rows are returned
        names = result["name"].to_list()
        self.assertIn("Charlie", names)
        self.assertIn("David", names)
    
    def test_edge_cases(self):
        """Test edge cases"""
        # Test with non-existent column
        result = apply_filters(self.df, [{"column": "non_existent", "operator": "eq", "value": "test"}])
        self.assertEqual(len(result), 5)  # Should return original DataFrame with warning
        
        # Test with invalid operator
        result = apply_filters(self.df, [{"column": "name", "operator": "invalid_op", "value": "test"}])
        self.assertEqual(len(result), 5)  # Should return original DataFrame with warning
        
        # Test with multiple filters where one is invalid
        result = apply_filters(self.df, [
            {"column": "non_existent", "operator": "eq", "value": "test"},
            {"column": "age", "operator": "gt", "value": 30}
        ])
        self.assertEqual(len(result), 3)  # Should skip invalid filter and apply valid one
    
    def test_all_operators(self):
        """Test all supported operators"""
        # Create a DataFrame with more test cases
        test_df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "value": ["abc", "def", "abc123", "123def", "", None, "start_text", "text_end"]
        })
        
        # Test eq (equality)
        result = apply_filters(test_df, [{"column": "value", "operator": "eq", "value": "abc"}])
        self.assertEqual(len(result), 1)
        
        # Test contains
        result = apply_filters(test_df, [{"column": "value", "operator": "contains", "value": "abc"}])
        self.assertEqual(len(result), 2)  # "abc", "abc123"
        
        # Test startsWith
        result = apply_filters(test_df, [{"column": "value", "operator": "startsWith", "value": "abc"}])
        self.assertEqual(len(result), 2)  # "abc", "abc123"
        
        # Test endsWith
        result = apply_filters(test_df, [{"column": "value", "operator": "endsWith", "value": "def"}])
        self.assertEqual(len(result), 2)  # "def", "123def"
        
        # Test notNull
        result = apply_filters(test_df, [{"column": "value", "operator": "notNull", "value": None}])
        self.assertEqual(len(result), 7)  # All except None
        
        # Test null
        result = apply_filters(test_df, [{"column": "value", "operator": "null", "value": None}])
        self.assertEqual(len(result), 1)  # Only None
        
        # Test notEmpty
        result = apply_filters(test_df, [{"column": "value", "operator": "notEmpty", "value": None}])
        self.assertEqual(len(result), 6)  # All except empty and None
        
        # Test empty
        result = apply_filters(test_df, [{"column": "value", "operator": "empty", "value": None}])
        self.assertEqual(len(result), 2)  # Empty and None

if __name__ == "__main__":
    unittest.main() 