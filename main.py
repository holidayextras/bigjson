from google.cloud.bigquery.table import SchemaField


def get_jsonschema_type(v):
    if isinstance(v, dict) and 'type' in v:
        return v['type']
    if isinstance(v, list):
        return v[0]
    return v


def get_format(v):
    if isinstance(v, dict) and 'format' in v:
        return v['format']
    if isinstance(v, list):
        return v[0]
    return None


def convert_json_to_bq_type(jsonschema_type, format=None, properties=None):
    to_replace = [
        ['number', 'float'],
        ['array', 'record'],
        ['object', 'record'],
        ['date-time', 'timestamp']
    ]
    if format:
        if 'date' not in format:
            return 'string'
        jsonschema_type = format
    data_type = jsonschema_type[0] if isinstance(jsonschema_type, list) else jsonschema_type
    if 'array' in jsonschema_type and 'string' == properties:
        return 'string'
    for i in to_replace:
        data_type = data_type.replace(i[0], i[1])
    return data_type


def get_field_mode(key, required, jsonschema_type):
    key = key[0] if isinstance(key, list) else key
    if 'array' in jsonschema_type:
        return 'repeated'
    if required:
        return 'required' if key in required else 'nullable'
    return 'nullable'


def get_properties(v, jsonschema_type):
    if 'array' in jsonschema_type:
        if 'items' in v:
            items = v['items']
            if 'properties' in items:
                return items['properties']
            if 'type' in items:
                return items['type']
    if isinstance(v.get('properties'), dict):
        return v.get('properties')
    return None


def recursive_object(properties, required_fields=None, jsonschema_type=None):
    to_recurse = ['array', 'object', 'record']
    result, nested_result = [], ()
    if not jsonschema_type:
        return 'error'
    for k, v in properties.items():
        if 'required' not in k:
            jsonschema_type = get_jsonschema_type(v)
            properties = get_properties(v, jsonschema_type)
            field_type = convert_json_to_bq_type(jsonschema_type, format=get_format(v), properties=properties)
            field_mode = get_field_mode(k, required_fields, jsonschema_type)
            field_description = v.get('description', '').replace('-', '') if isinstance(v, dict) else ''

            if field_type in to_recurse and isinstance(properties, dict):
                nested_result = recursive_object(properties,
                                                 jsonschema_type=jsonschema_type) if properties else ()
            result.append(SchemaField(k,
                                      field_type,
                                      mode=field_mode,
                                      description=field_description,
                                      fields=nested_result))
            # flush nested_result otherwise it will be present in the next loop
            nested_result = ()
    return result