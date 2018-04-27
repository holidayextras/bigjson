import copy
import functools
import json
import shutil
import sys
from google.cloud import bigquery
from pprint import pprint as _pprint


pprint = functools.partial(_pprint, width=shutil.get_terminal_size().columns)


JSON_SCHEMA_TO_BIGQUERY_TYPE_DICT = {
    'boolean': 'BOOLEAN',
    'date-time': 'TIMESTAMP',
    'integer': 'INTEGER',
    'number': 'FLOAT',
    'string': 'STRING'
}


def merge_property(merge_type, property_name, destination_value, source_value):
    """Merges two properties.

    """
    if destination_value is None and source_value is None:
        return None
    if destination_value is None:
        return source_value
    if source_value is None:
        return destination_value
    if isinstance(destination_value, dict) and isinstance(source_value, dict):
        return merge_dicts(merge_type, destination_value, source_value)
    if isinstance(destination_value, list):
        destination_list = copy.copy(destination_value)
    else:
        destination_list = [destination_value]
    if isinstance(source_value, list):
        source_list = source_value
    else:
        source_list = [source_value]
    if property_name == 'required' and merge_type in ['anyOf', 'oneOf']:
        return [v for v in destination_list if v in source_list]
    destination_list.extend([v for v in source_list if v not in destination_list])
    return destination_list


def merge_dicts(merge_type, *dicts):
    """Deep merges two dictionaries.

    """
    result = copy.deepcopy(dicts[0])
    for dict_ in dicts[1:]:
        for name in dict_.keys():
            merged_property = merge_property(merge_type, name, result.get(name), dict_.get(name))
            if merged_property is not None:
                result[name] = merged_property
    return result


def scalar(name, type_, mode, description):
    bigquery_type = JSON_SCHEMA_TO_BIGQUERY_TYPE_DICT[type_]
    return bigquery.SchemaField(name, bigquery_type, mode, description)


def array(name, node, mode):
    items_with_description = copy.deepcopy(node['items'])
    if 'description' not in items_with_description:
        items_with_description['description'] = node.get('description')
    return visit(name, items_with_description, 'REPEATED')


def object_(name, node, mode):
    required_properties = node.get('required', {})
    properties = node.get('properties', {})
    fields = tuple([visit(key, value, 'REQUIRED' if key in required_properties else 'NULLABLE')
                    for key, value in properties.items()])
    return bigquery.SchemaField(name, 'RECORD', mode, node.get('description'), fields)


def simple(name, type_, node, mode):
    if type_ == 'array':
        return array(name, node, mode)
    if type_ == 'object':
        return object_(name, node, mode)
    actual_type = type_
    format_ = node.get('format')
    if type_ == 'string' and format_ == 'date-time':
        actual_type = format_
    return scalar(name, actual_type, mode, node.get('description'))


def visit(name, node, mode='NULLABLE'):
    merged_node = node
    for x_of in ['allOf', 'anyOf', 'oneOf']:
        if x_of in node:
            merged_node = merge_dicts(x_of, node, *node[x_of])
            del merged_node[x_of]
    type_ = merged_node['type']
    actual_mode = mode
    if isinstance(type_, list):
        non_null_types = [scalar_type for scalar_type in type_ if scalar_type != 'null']
        if len(non_null_types) > 1:
            raise Exception(f'union type not supported: {node}')
        if 'null' in type_:
            actual_mode = 'NULLABLE'
        type_ = non_null_types[0]
    result = simple(name, type_, merged_node,  actual_mode)
    return result


def convert(input_schema):
    return list(visit('root', input_schema).fields)


def get_table_id(schema):
    id = schema['id'].split('/')
    name = id[-4:-2]
    version = id[-2].split('.')[0]
    return '_'.join(name + [version])

def run(input_schema, project):
    output_schema = convert(input_schema)
    pprint([field.to_api_repr() for field in output_schema])
    if not project:
         return

    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset('collector__streaming')
    table_id = get_table_id(input_schema)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table.schema = output_schema
    table.friendly_name = input_schema.get('title')
    table.description = input_schema.get('description') or input_schema.get('title')
    table.partitioning_type = 'DAY'
    table = bigquery_client.create_table(table)
    print(f'Created table {table_id} in dataset {dataset_ref} of project {project}.')

if __name__ == '__main__':
    input_schema = json.load(sys.stdin)
    project = (len(sys.argv) > 1 and sys.argv[1])
    run(input_schema, project)
