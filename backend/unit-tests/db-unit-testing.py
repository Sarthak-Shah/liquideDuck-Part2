import unittest
import os, sys

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import init_db, update_cell, get_all_cells, close_db, print_table_schema


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.conn = init_db()
        print_table_schema(self.conn)  # Print the table schema for debugging

    def tearDown(self):
        close_db(self.conn)

    def test_update_and_retrieve_cell(self):
        # Insert a new cell
        update_cell(self.conn, 1, 1, "Hello")
        cells = get_all_cells(self.conn)
        self.assertEqual(cells, [(1, 1, "Hello")])

        # Update the cell
        update_cell(self.conn, 1, 1, "World")
        cells = get_all_cells(self.conn)
        self.assertEqual(cells, [(1, 1, "World")])


if __name__ == "__main__":
    unittest.main()