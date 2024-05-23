import unittest
from unittest.mock import patch
from ingestor import (
    db_saves,
    ddl_schema,
    generate_ddl,
    get_secrets,
    is_uniform_dicts,
    get_value,
    save_to_db,
    validate_table_name,
    kv_saves,
    flatten_and_stringify,
)


class TestFunctions(unittest.TestCase):
    def test_flatten_and_stringify(self):
        obj = {
            "name": "John Doe",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "CA",
                "coordinates": {"lat": 10, "long": 20},
            },
            "hobbies": ["reading", "hiking", "cooking"],
        }
        actual_output = flatten_and_stringify(obj)
        expected_output = {
            "name": "John Doe",
            "age": "30",
            "address_street": "123 Main St",
            "address_city": "Anytown",
            "address_state": "CA",
            "address_coordinates": '{"lat": 10, "long": 20}',
            "hobbies": '["reading", "hiking", "cooking"]',
            "livedocs_date": actual_output["livedocs_date"],
        }
        self.assertEqual(actual_output, expected_output)

    def test_is_uniform_dicts(self):
        data = [
            {"name": "John Doe", "age": 30},
            {"name": "Jane Smith", "age": 25},
            {"name": "Bob Johnson", "age": 35},
        ]
        uniform, schema = is_uniform_dicts(data)
        self.assertTrue(uniform)
        self.assertEqual(schema, {"name": "str", "age": "int"})

        data = [
            {"name": "John Doe", "age": 30},
            {"name": "Jane Smith", "age": 25, "email": "jane@example.com"},
            {"name": "Bob Johnson", "age": 35},
        ]
        uniform, schema = is_uniform_dicts(data)
        self.assertFalse(uniform)
        self.assertEqual(schema, {"name": "str", "age": "int"})

        data = []
        uniform, schema = is_uniform_dicts(data)
        self.assertTrue(uniform)
        self.assertIsNone(schema)

    @patch("builtins.print")
    def test_save_to_db(self, mock_print):
        # Test saving valid data
        data = [
            {"name": "John Doe", "age": 30},
            {"name": "Jane Smith", "age": 25},
            {"name": "Bob Johnson", "age": 35},
        ]
        save_to_db("users", data)
        self.assertEqual(
            db_saves["users"],
            [
                {
                    "name": "John Doe",
                    "age": "30",
                    "livedocs_date": db_saves["users"][0]["livedocs_date"],
                },
                {
                    "name": "Jane Smith",
                    "age": "25",
                    "livedocs_date": db_saves["users"][1]["livedocs_date"],
                },
                {
                    "name": "Bob Johnson",
                    "age": "35",
                    "livedocs_date": db_saves["users"][2]["livedocs_date"],
                },
            ],
        )
        self.assertIn("users", ddl_schema)
        self.assertEqual(
            ddl_schema["users"],
            "CREATE TABLE users (name STRING, age INT64, livedocs_date TIMESTAMP);",
        )

        data = [
            {"name": "John Doe", "age": 30},
            {"name": "Jane Smith", "age": 25, "email": "jane@example.com"},
            {"name": "Bob Johnson", "age": 35},
        ]
        save_to_db("users", data)
        self.assertEqual(db_saves["users"], "INVALID")
        self.assertEqual(ddl_schema["users"], "INVALID")

        # Test saving non-list data
        save_to_db("users", {"name": "John Doe", "age": 30})
        self.assertEqual(db_saves["users"], "INVALID")
        self.assertEqual(ddl_schema["users"], "INVALID")

        # Test saving to an already invalid table
        db_saves["users"] = "INVALID"
        save_to_db("users", data)
        self.assertEqual(db_saves["users"], "INVALID")
        self.assertEqual(ddl_schema["users"], "INVALID")

        mock_print.assert_not_called()

    def test_validate_table_name(self):
        self.assertTrue(validate_table_name("valid_table_name")[0])
        self.assertFalse(validate_table_name("invalid table name")[0])
        self.assertFalse(validate_table_name("invalid-table-name")[0])
        self.assertFalse(validate_table_name("a" * 1025)[0])

    def test_generate_ddl(self):
        schema_dict = {
            "name": "str",
            "age": "int",
            "is_active": "bool",
            "created_at": "datetime",
            "coordinates": "dict",
            "hobbies": "list",
        }
        ddl = generate_ddl(schema_dict, "users")
        expected_ddl = (
            "CREATE TABLE users (name STRING, age INT64, is_active BOOL, created_at TIMESTAMP, "
            "coordinates JSON, hobbies JSON, livedocs_date TIMESTAMP);"
        )
        self.assertEqual(ddl, expected_ddl)

    # def test_get_secrets(self):
    #     secrets = get_secrets()
    #     print(secrets)

    # def test_kv(self):
    #     print(kv_saves)
    #     val = get_value("key")
    #     print(val)


if __name__ == "__main__":
    unittest.main()
