# BigJson

A small library to parse json schemas and output to big query schema only, for now

## Usage

Until this is turned into a package this is the simple usage

```python
from main import recursive_object

input_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "id": "/models/error/v2.0.0/schema.json",
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    },
    "source": {
      "description": "type of the source the error was generated on",
      "enum": [
        "client",
        "server"
      ]
    },
    "code": {
      "description": "error code of the language used",
      "type": "string",
      "pattern": "^[\\da-z\\-]{3,20}$"
    },
    "info": {
      "description": "additional info about the error",
      "type": "string"
    }
  },
  "required": [
    "message",
    "code",
    "source"
  ],
  "additionalProperties": False,
}

bq_schema = recursive_object(input_schema['properties'],
                            required_fields=input_schema['required'],
                            jsonschema_type=input_schema['required'])
```