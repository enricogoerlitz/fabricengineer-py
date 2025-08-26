import json
import base64

from fabricengineer.api.utils import base64_encode


def test_base64_encode_simple_dict():
    """Test encoding a simple dictionary."""
    input_dict = {"key": "value"}
    result = base64_encode(input_dict)

    # Decode and verify the result
    decoded_bytes = base64.b64decode(result.encode("ascii"))
    decoded_json = json.loads(decoded_bytes.decode("utf-8"))

    assert decoded_json == input_dict
    assert isinstance(result, str)


def test_base64_encode_empty_dict():
    """Test encoding an empty dictionary."""
    input_dict = {}
    result = base64_encode(input_dict)

    # Decode and verify the result
    decoded_bytes = base64.b64decode(result.encode("ascii"))
    decoded_json = json.loads(decoded_bytes.decode("utf-8"))

    assert decoded_json == input_dict
    assert isinstance(result, str)


def test_base64_encode_nested_dict():
    """Test encoding a nested dictionary."""
    input_dict = {
        "user": {
            "name": "John Doe",
            "age": 30,
            "preferences": {
                "theme": "dark",
                "language": "en"
            }
        },
        "settings": ["option1", "option2"]
    }
    result = base64_encode(input_dict)

    # Decode and verify the result
    decoded_bytes = base64.b64decode(result.encode("ascii"))
    decoded_json = json.loads(decoded_bytes.decode("utf-8"))

    assert decoded_json == input_dict
    assert isinstance(result, str)


def test_base64_encode_with_special_characters():
    """Test encoding dictionary with special characters."""
    input_dict = {
        "message": "Hello, 世界! 🌍",
        "symbols": "äöüß@#$%^&*()",
        "unicode": "café naïve résumé"
    }
    result = base64_encode(input_dict)

    # Decode and verify the result
    decoded_bytes = base64.b64decode(result.encode("ascii"))
    decoded_json = json.loads(decoded_bytes.decode("utf-8"))

    assert decoded_json == input_dict
    assert isinstance(result, str)


def test_base64_encode_with_numbers_and_booleans():
    """Test encoding dictionary with various data types."""
    input_dict = {
        "integer": 42,
        "float": 3.14159,
        "boolean_true": True,
        "boolean_false": False,
        "null_value": None,
        "list": [1, 2, 3, "text"],
        "string": "test"
    }
    result = base64_encode(input_dict)

    # Decode and verify the result
    decoded_bytes = base64.b64decode(result.encode("ascii"))
    decoded_json = json.loads(decoded_bytes.decode("utf-8"))

    assert decoded_json == input_dict
    assert isinstance(result, str)
