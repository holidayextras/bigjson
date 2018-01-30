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


def merge_dicts(*dicts):
    """Deep merges two dictionaries.

    """
    result = copy.deepcopy(dicts[0])
    for dict_ in dicts[1:]:
        dict_copy = copy.deepcopy(dict_)
        for k in dict_copy.keys():
            if k in result:
                if isinstance(result[k], dict):
                    result[k].update(dict_copy[k])
                else:
                    if not isinstance(result[k], list):
                        result[k] = [result[k]]
                    if isinstance(dict_copy[k], list):
                        result[k].extend([v for v in dict_copy[k] if v not in result[k]])
                    else:
                        if dict_copy[k] not in result[k]:
                            result[k].append(dict_copy[k])
            else:
                result[k] = dict_copy[k]
    return result


def scalar(name, type_, mode, description):
    bigquery_type = JSON_SCHEMA_TO_BIGQUERY_TYPE_DICT[type_]
    return bigquery.SchemaField(name, bigquery_type, mode, description)


def array(name, node, mode):
    items_with_description = copy.deepcopy(node['items'])
    if 'description' not in items_with_description:
        items_with_description['description'] = node['description']
    return visit(name, items_with_description, 'REPEATED')


def object_(name, node, mode):
    required_properties = node.get('required', {})
    fields = tuple([visit(key, value, 'REQUIRED' if key in required_properties else 'NULLABLE')
                    for key, value in node['properties'].items()])
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
            merged_node = merge_dicts(node, *node[x_of])
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
    pprint(result.to_api_repr())
    return result


def convert(input_schema):
    return list(visit('root', input_schema).fields)


def get_table_id(schema):
    id = schema['id'].split('/')
    name = id[-4:-2]
    version = id[-2].split('.')[0]
    return '_'.join(name + [version])


if __name__ == '__main__':
    input_schema = json.load(sys.stdin)
    output_schema = convert(input_schema)
    pprint([field.to_api_repr() for field in output_schema])
    bigquery_client = bigquery.Client(project='hx-trial')
    dataset_ref = bigquery_client.dataset('collector__streaming')
    table_id = get_table_id(input_schema)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table.schema = output_schema
    table.friendly_name = input_schema.get('title')
    table.description = input_schema.get('description') or input_schema.get('title')
    table.partitioning_type = 'DAY'
    table = bigquery_client.create_table(table)
    print(f'Created table {table_id} in dataset {dataset_ref}.')
