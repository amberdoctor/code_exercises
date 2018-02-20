import unittest
import os.path
import sys
import io

from collections import OrderedDict
from csv_etl_dag import aggregate_top_in_field, \
    print_top_in_field, \
    apply_column_filter, \
    group_by_column, \
    reverse_field_order_in_ordered_dict_list, \
    save_to_output_directory_by_field, \
    read_csv_to_list_of_ordered_dicts


class TestCSVETLDAG(unittest.TestCase):

    def setUp(self):
        self.test_result = None

    def record(self, final_result):
        self.test_result = final_result

    def get_contents_of_file(self, filepath):
        with open(filepath, 'r') as file:
            file_contents = file.read()
            return str(file_contents)


    def test_aggregate_top_in_field_with_default_param(self):
        name_of_field = 'Name'
        field_value_1 = 'foo1'
        field_value_2 = 'foo2'
        field_value_3 = 'foo3'
        field_value_4 = 'foo4'

        test_value = [OrderedDict({'Status': 'Active', name_of_field: field_value_1, 'Target Type': 'Bar0'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_2, 'Target Type': 'Bar1'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_3, 'Target Type': 'Bar2'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_3, 'Target Type': 'Bar3'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_4, 'Target Type': 'Bar4'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_3, 'Target Type': 'Bar5'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_2, 'Target Type': 'Bar6'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_1, 'Target Type': 'Bar7'})]
        expected_result = {'fieldname':name_of_field, 'counts':{field_value_1:2,
                                                                field_value_2:2,
                                                                field_value_3:3,
                                                                field_value_4:1}}
        test_filter_fn = aggregate_top_in_field([self.record])
        test_filter_fn(test_value)

        self.assertEqual(expected_result['fieldname'], self.test_result['fieldname'])
        self.assertEqual(expected_result['counts'], self.test_result['counts'])


    def test_aggregate_top_in_field_with_non_default_param(self):
        name_of_field = 'Foo Number'
        field_value_1 = 'foo1'
        field_value_2 = 'foo2'
        field_value_3 = 'foo3'
        field_value_4 = 'foo4'

        test_value = [OrderedDict({'Status': 'Active', name_of_field: field_value_1, 'Target Type': 'Bar0'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_2, 'Target Type': 'Bar1'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_3, 'Target Type': 'Bar2'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_3, 'Target Type': 'Bar3'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_4, 'Target Type': 'Bar4'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_3, 'Target Type': 'Bar5'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_2, 'Target Type': 'Bar6'}),
                      OrderedDict({'Status': 'Active', name_of_field: field_value_1, 'Target Type': 'Bar7'})]
        expected_result = {'fieldname':name_of_field, 'counts':{field_value_1:2,
                                                                field_value_2:2,
                                                                field_value_3:3,
                                                                field_value_4:1}}
        test_filter_fn = aggregate_top_in_field([self.record], name_of_field=name_of_field)
        test_filter_fn(test_value)

        self.assertEqual(expected_result['fieldname'], self.test_result['fieldname'])
        self.assertEqual(expected_result['counts'], self.test_result['counts'])


    def test_print_top_in_field_with_non_default_param(self):
        test_value = {'fieldname':'Name', 'counts':{'foo1':2,
                                                    'foo2':2,
                                                    'foo3':3,
                                                    'foo4':1}}
        expected_value = 'Printing the 3 most often repeated values from the "Name" column.\nfoo3\nfoo1\nfoo2\n'

        stdout_ = sys.stdout  # IMPORTANT: track of the current stdout
        stream = io.StringIO()
        sys.stdout = stream

        test_filter_fn = print_top_in_field([self.record], number_to_print=3)
        test_filter_fn(test_value)

        sys.stdout = stdout_  #IMPORTANT: restore the previous stdout
        actual_value = stream.getvalue()
        self.assertEqual(expected_value, actual_value)

        self.assertEqual(test_value['fieldname'], self.test_result['fieldname'])
        self.assertEqual(test_value['counts'], self.test_result['counts'])


    def test_print_top_in_field_with_non_default_param_and_less_than_number(self):
        test_value = {'fieldname':'Name', 'counts':{'foo1':2,
                                                    'foo2':2,
                                                    'foo3':3,
                                                    'foo4':1}}
        expected_value = 'Printing the 10 most often repeated values from the "Name" column.\nfoo3\nfoo1\nfoo2\nfoo4\nNotice: There were less names than the requested number to print.\n'

        stdout_ = sys.stdout  # IMPORTANT: track of the current stdout
        stream = io.StringIO()
        sys.stdout = stream

        test_filter_fn = print_top_in_field([self.record])
        test_filter_fn(test_value)

        sys.stdout = stdout_  #IMPORTANT: restore the previous stdout
        actual_value = stream.getvalue()
        self.assertEqual(expected_value, actual_value)

        self.assertEqual(test_value['fieldname'], self.test_result['fieldname'])
        self.assertEqual(test_value['counts'], self.test_result['counts'])


    def test_print_top_in_field_with_exact_number(self):
        test_value = {'fieldname': 'Name', 'counts': {'foo1': 2,
                                                      'foo2': 2,
                                                      'foo3': 3,
                                                      'foo4': 1}}
        expected_value = 'Printing the 4 most often repeated values from the "Name" column.\nfoo3\nfoo1\nfoo2\nfoo4\n'

        stdout_ = sys.stdout  # IMPORTANT: track of the current stdout
        stream = io.StringIO()
        sys.stdout = stream

        test_filter_fn = print_top_in_field([self.record], number_to_print=4)
        test_filter_fn(test_value)

        sys.stdout = stdout_  # IMPORTANT: restore the previous stdout
        actual_value = stream.getvalue()
        self.assertEqual(expected_value, actual_value)

        self.assertEqual(test_value['fieldname'], self.test_result['fieldname'])
        self.assertEqual(test_value['counts'], self.test_result['counts'])


    def test_print_top_in_field_with_tie_for_last_spot(self):
        test_value = {'fieldname': 'Name', 'counts': {'foo1': 2,
                                                      'foo2': 2,
                                                      'foo3': 3,
                                                      'foo4': 1}}
        expected_value = 'Printing the 2 most often repeated values from the "Name" column.\nfoo3\nfoo1\n'

        stdout_ = sys.stdout  # IMPORTANT: track of the current stdout
        stream = io.StringIO()
        sys.stdout = stream

        test_filter_fn = print_top_in_field([self.record], number_to_print=2)
        test_filter_fn(test_value)

        sys.stdout = stdout_  # IMPORTANT: restore the previous stdout
        actual_value = stream.getvalue()
        self.assertEqual(expected_value, actual_value)

        self.assertEqual(test_value['fieldname'], self.test_result['fieldname'])
        self.assertEqual(test_value['counts'], self.test_result['counts'])


    def test_apply_column_filter_removes_when_column_name_does_not_match_value(self):
        test_value = [OrderedDict({'Status': 'Inactive', 'Foo': 'Baz'})]
        test_filter_fn = apply_column_filter([self.record], column_name='Status', value='Active')
        test_filter_fn(test_value)

        self.assertEqual(0, len(self.test_result))


    def test_apply_column_filter_keeps_row_when_column_name_matches_value(self):
        test_value = [OrderedDict({'Status': 'Active', 'Foo': 'Bar'})]
        expected_result = [OrderedDict({'Status': 'Active', 'Foo': 'Bar'})]
        test_filter_fn = apply_column_filter([self.record], column_name='Status', value='Active')
        test_filter_fn(test_value)

        self.assertEqual(1, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])


    def test_apply_column_filter_with_default_params(self):
        test_value = [OrderedDict({'Status': 'Active', 'Foo': 'Bar'}),
                      OrderedDict({'Status': 'Inactive', 'Foo': 'Baz'})]
        expected_result = [OrderedDict({'Status': 'Active', 'Foo': 'Bar'})]
        test_filter_fn = apply_column_filter([self.record])
        test_filter_fn(test_value)

        self.assertEqual(1, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])


    def test_apply_column_filter_with_non_default_params(self):
        test_value = [OrderedDict({'Greeting': 'Hello', 'Foo': 'Bar'}),
                      OrderedDict({'Greeting': 'Hi', 'Foo': 'Baz'})]
        expected_result = [OrderedDict({'Greeting': 'Hello', 'Foo': 'Bar'})]
        test_filter_fn = apply_column_filter([self.record], column_name='Foo', value='Bar')
        test_filter_fn(test_value)

        self.assertEqual(1, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])


    def test_group_by_column_with_default_param(self):
        test_value = [OrderedDict({'Status': 'Active', 'Foo': 'Bar0', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar1', 'Target Type': 'State'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar2', 'Target Type': 'County'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar3', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar4', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar5', 'Target Type': 'State'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar6', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar7', 'Target Type': 'Country'})]
        expected_result = [OrderedDict({'Status': 'Active', 'Foo': 'Bar0', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar3', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar4', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar6', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar1', 'Target Type': 'State'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar5', 'Target Type': 'State'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar2', 'Target Type': 'County'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar7', 'Target Type': 'Country'})]
        test_filter_fn = group_by_column([self.record])
        test_filter_fn(test_value)

        self.assertEqual(8, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])
        self.assertEqual(expected_result[1], self.test_result[1])
        self.assertEqual(expected_result[2], self.test_result[2])
        self.assertEqual(expected_result[3], self.test_result[3])
        self.assertEqual(expected_result[4], self.test_result[4])
        self.assertEqual(expected_result[5], self.test_result[5])
        self.assertEqual(expected_result[6], self.test_result[6])
        self.assertEqual(expected_result[7], self.test_result[7])


    def test_group_by_column_with_non_default_param(self):
        test_value = [OrderedDict({'Status': 'Active', 'Foo': 'Bar0', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar1', 'Target Type': 'State'}),
                      OrderedDict({'Status': 'Inactive', 'Foo': 'Bar2', 'Target Type': 'County'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar3', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Inactive', 'Foo': 'Bar4', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar5', 'Target Type': 'State'}),
                      OrderedDict({'Status': 'Inactive', 'Foo': 'Bar6', 'Target Type': 'City'}),
                      OrderedDict({'Status': 'Active', 'Foo': 'Bar7', 'Target Type': 'Country'})]
        expected_result = [OrderedDict({'Status': 'Active', 'Foo': 'Bar0', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar1', 'Target Type': 'State'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar3', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar5', 'Target Type': 'State'}),
                          OrderedDict({'Status': 'Active', 'Foo': 'Bar7', 'Target Type': 'Country'}),
                          OrderedDict({'Status': 'Inactive', 'Foo': 'Bar2', 'Target Type': 'County'}),
                          OrderedDict({'Status': 'Inactive', 'Foo': 'Bar4', 'Target Type': 'City'}),
                          OrderedDict({'Status': 'Inactive', 'Foo': 'Bar6', 'Target Type': 'City'})]
        test_filter_fn = group_by_column([self.record], column_name='Status')
        test_filter_fn(test_value)

        self.assertEqual(8, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])
        self.assertEqual(expected_result[1], self.test_result[1])
        self.assertEqual(expected_result[2], self.test_result[2])
        self.assertEqual(expected_result[3], self.test_result[3])
        self.assertEqual(expected_result[4], self.test_result[4])
        self.assertEqual(expected_result[5], self.test_result[5])
        self.assertEqual(expected_result[6], self.test_result[6])
        self.assertEqual(expected_result[7], self.test_result[7])


    def test_reverse_field_order_in_ordered_dict_list_reverses_one(self):
        test_value = [OrderedDict([('Status', 'Active'), ('Foo', 'Bar'), ('Target Type', 'City'), ('Greeting','hi')])]
        expected_result = [OrderedDict([('Greeting','hi'), ('Target Type', 'City'), ('Foo', 'Bar'), ('Status', 'Active')])]
        test_filter_fn = reverse_field_order_in_ordered_dict_list([self.record])
        test_filter_fn(test_value)

        self.assertEqual(1, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])


    def test_reverse_field_order_in_ordered_dict_list_reverses_multiple(self):
        test_value = [OrderedDict([('Status', 'Active'), ('Foo', 'Bar0'), ('Target Type', 'City'), ('Greeting','hi')]),
                      OrderedDict([('Status', 'Active'), ('Foo', 'Bar1'), ('Target Type', 'City'), ('Greeting', 'hi')]),
                      OrderedDict([('Status', 'Active'), ('Foo', 'Bar2'), ('Target Type', 'City'), ('Greeting', 'hi')]),
                      OrderedDict([('Status', 'Active'), ('Foo', 'Bar3'), ('Target Type', 'City'), ('Greeting', 'hi')]),
                      OrderedDict([('Status', 'Active'), ('Foo', 'Bar4'), ('Target Type', 'City'), ('Greeting', 'hi')])]
        expected_result = [OrderedDict([('Greeting','hi'), ('Target Type', 'City'), ('Foo', 'Bar0'), ('Status', 'Active')]),
                           OrderedDict([('Greeting', 'hi'), ('Target Type', 'City'), ('Foo', 'Bar1'), ('Status', 'Active')]),
                           OrderedDict([('Greeting', 'hi'), ('Target Type', 'City'), ('Foo', 'Bar2'), ('Status', 'Active')]),
                           OrderedDict([('Greeting', 'hi'), ('Target Type', 'City'), ('Foo', 'Bar3'), ('Status', 'Active')]),
                           OrderedDict([('Greeting', 'hi'), ('Target Type', 'City'), ('Foo', 'Bar4'), ('Status', 'Active')])]
        test_filter_fn = reverse_field_order_in_ordered_dict_list([self.record])
        test_filter_fn(test_value)

        self.assertEqual(5, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])
        self.assertEqual(expected_result[1], self.test_result[1])
        self.assertEqual(expected_result[2], self.test_result[2])
        self.assertEqual(expected_result[3], self.test_result[3])
        self.assertEqual(expected_result[4], self.test_result[4])


    def test_save_to_output_directory_by_field_with_non_default_param(self):
        folderpath = './test_created_files'
        value_1 = 'City'
        value_2 = 'Country'
        value_3 = 'State'
        fieldname = 'Target Type'
        test_value = [
            OrderedDict([('Foo', 'Bar0'), (fieldname, value_1), ('Greeting', 'Hello,World'), ('Number', '42')]),
            OrderedDict([('Foo', 'Bar1'), (fieldname, value_1), ('Greeting', 'Hello,World'), ('Number', '9001')]),
            OrderedDict([('Foo', 'Bar2'), (fieldname, value_2), ('Greeting', 'Hi'), ('Number', '525600')]),
            OrderedDict([('Foo', 'Bar3'), (fieldname, value_2), ('Greeting', 'Hey Man'), ('Number', '9')]),
            OrderedDict([('Foo', 'Bar4'), (fieldname, value_2), ('Greeting', 'Hey Man'), ('Number', '10')]),
            OrderedDict([('Foo', 'Bar5'), (fieldname, value_3), ('Greeting', 'Hey Man'), ('Number', '64')])]
        test_filter_fn = save_to_output_directory_by_field([self.record], folderpath)
        test_filter_fn(test_value)

        expected_result_file_1 = 'Foo,Target Type,Greeting,Number\nBar0,City,"Hello,World",42\nBar1,City,"Hello,World",9001\n'
        expected_result_file_2 = 'Foo,Target Type,Greeting,Number\nBar2,Country,Hi,525600\n'
        expected_result_file_3 = 'Foo,Target Type,Greeting,Number\nBar3,Country,Hey Man,9\nBar4,Country,Hey Man,10\nBar5,State,Hey Man,64\n'

        filepath_1 = os.path.join(folderpath, '{0}.csv'.format(value_1))
        filepath_2 = os.path.join(folderpath, '{0}.csv'.format(value_2))
        filepath_3 = os.path.join(folderpath, '{0}.csv'.format(value_3))

        actual_result_file_1 = self.get_contents_of_file(filepath_1)
        actual_result_file_2 = self.get_contents_of_file(filepath_2)
        actual_result_file_3 = self.get_contents_of_file(filepath_3)

        self.assertEqual(expected_result_file_1, actual_result_file_1)
        self.assertEqual(expected_result_file_2, actual_result_file_2)
        self.assertEqual(expected_result_file_3, actual_result_file_3)

        # Data being passed to list of functions does not change
        self.assertEqual(6, len(self.test_result))
        self.assertEqual(test_value[0], self.test_result[0])
        self.assertEqual(test_value[1], self.test_result[1])
        self.assertEqual(test_value[2], self.test_result[2])
        self.assertEqual(test_value[3], self.test_result[3])
        self.assertEqual(test_value[4], self.test_result[4])
        self.assertEqual(test_value[5], self.test_result[5])

        # Delete the test files
        for file in [filepath_1, filepath_2, filepath_3]:
            os.remove(file)


    def test_save_to_output_directory_by_field_with_non_default_param(self):
        folderpath = './test_created_files'
        value_1 = 'Hello,World'
        value_2 = 'Hi'
        value_3 = 'Hey Man'
        fieldname = 'Greeting'
        test_value = [
            OrderedDict([('Foo', 'Bar0'), ('Target Type', 'City'), (fieldname, value_1), ('Number', '42')]),
            OrderedDict([('Foo', 'Bar1'), ('Target Type', 'City'), (fieldname, value_1), ('Number', '9001')]),
            OrderedDict([('Foo', 'Bar2'), ('Target Type', 'Country'), (fieldname, value_2), ('Number', '525600')]),
            OrderedDict([('Foo', 'Bar3'), ('Target Type', 'Country'), (fieldname, value_3), ('Number', '9')]),
            OrderedDict([('Foo', 'Bar4'), ('Target Type', 'Country'), (fieldname, value_3), ('Number', '10')]),
            OrderedDict([('Foo', 'Bar5'), ('Target Type', 'State'), (fieldname, value_3), ('Number', '64')])]
        test_filter_fn = save_to_output_directory_by_field([self.record], folderpath, fieldname='Greeting')
        test_filter_fn(test_value)

        expected_result_file_1 = 'Foo,Target Type,Greeting,Number\nBar0,City,"Hello,World",42\nBar1,City,"Hello,World",9001\n'
        expected_result_file_2 = 'Foo,Target Type,Greeting,Number\nBar2,Country,Hi,525600\n'
        expected_result_file_3 = 'Foo,Target Type,Greeting,Number\nBar3,Country,Hey Man,9\nBar4,Country,Hey Man,10\nBar5,State,Hey Man,64\n'

        filepath_1 = os.path.join(folderpath, '{0}.csv'.format(value_1))
        filepath_2 = os.path.join(folderpath, '{0}.csv'.format(value_2))
        filepath_3 = os.path.join(folderpath, '{0}.csv'.format(value_3))

        actual_result_file_1 = self.get_contents_of_file(filepath_1)
        actual_result_file_2 = self.get_contents_of_file(filepath_2)
        actual_result_file_3 = self.get_contents_of_file(filepath_3)

        self.assertEqual(expected_result_file_1, actual_result_file_1)
        self.assertEqual(expected_result_file_2, actual_result_file_2)
        self.assertEqual(expected_result_file_3, actual_result_file_3)

        # Data being passed to list of functions does not change
        self.assertEqual(6, len(self.test_result))
        self.assertEqual(test_value[0], self.test_result[0])
        self.assertEqual(test_value[1], self.test_result[1])
        self.assertEqual(test_value[2], self.test_result[2])
        self.assertEqual(test_value[3], self.test_result[3])
        self.assertEqual(test_value[4], self.test_result[4])
        self.assertEqual(test_value[5], self.test_result[5])

        # delete the test files
        for file in [filepath_1, filepath_2, filepath_3]:
            os.remove(file)


    def test_read_csv_to_list_of_ordered_dicts(self):
        test_value = os.path.join('.', 'test.csv')
        expected_result = [
            OrderedDict([('Status', 'Active'), ('Name', 'Bar0'), ('Target Type', 'City'), ('Greeting', 'Hello,World'), ('Number', '42')]),
            OrderedDict([('Status', 'Inactive'), ('Name', 'Bar1'), ('Target Type', 'City'), ('Greeting', 'Hi'), ('Number', '9001')]),
            OrderedDict([('Status', 'Doing Good'), ('Name', 'Bar2'), ('Target Type', 'Country'), ('Greeting', 'Hey Man'), ('Number', '525600')]),
            OrderedDict([('Status', 'Active'), ('Name', 'Bar3'), ('Target Type', 'City'), ('Greeting', 'Hello,World'), ('Number', '1')]),
            OrderedDict([('Status', 'Inactive'), ('Name', 'Bar4'), ('Target Type', 'City'), ('Greeting', 'Hi'), ('Number', '9002')]),
            OrderedDict([('Status', 'Doing Good'), ('Name', 'Bar5'), ('Target Type', 'Country'), ('Greeting', 'Hey Man'), ('Number', '96')]),
            OrderedDict([('Status', 'Doing Good'), ('Name', 'Bar6'), ('Target Type', 'Country'), ('Greeting', 'Hey Man'), ('Number', '121')]),
            OrderedDict([('Status', 'Inactive'), ('Name', 'Bar7'), ('Target Type', 'City'), ('Greeting', 'Hello,World'), ('Number', '10')]),
            OrderedDict([('Status', 'Active'), ('Name', 'Bar8'), ('Target Type', 'City'), ('Greeting', 'Hi'), ('Number', '9003')]),
            OrderedDict([('Status', 'Active'), ('Name', 'Bar9'), ('Target Type', 'Country'), ('Greeting', 'Hey Man'), ('Number', '67')]),
            OrderedDict([('Status', 'Doing Good'), ('Name', 'Bar10'), ('Target Type', 'City'), ('Greeting', 'Hello,World'), ('Number', '97')]),
            OrderedDict([('Status', 'Active'), ('Name', 'Bar11'), ('Target Type', 'City'), ('Greeting', 'Hi'), ('Number', '9004')]),
            OrderedDict([('Status', 'Active'), ('Name', 'Bar12'), ('Target Type', 'Country'), ('Greeting', 'Hey Man'), ('Number', '525')]),
            OrderedDict([('Status', 'Active'), ('Name', 'Bar13'), ('Target Type', 'Country'), ('Greeting', 'Hey Man'), ('Number', '5600')])]

        test_filter_fn = read_csv_to_list_of_ordered_dicts([self.record])
        test_filter_fn(test_value)

        self.assertEqual(14, len(self.test_result))
        self.assertEqual(expected_result[0], self.test_result[0])
        self.assertEqual(expected_result[1], self.test_result[1])
        self.assertEqual(expected_result[2], self.test_result[2])
        self.assertEqual(expected_result[3], self.test_result[3])
        self.assertEqual(expected_result[4], self.test_result[4])
        self.assertEqual(expected_result[5], self.test_result[5])
        self.assertEqual(expected_result[6], self.test_result[6])
        self.assertEqual(expected_result[7], self.test_result[7])
        self.assertEqual(expected_result[8], self.test_result[8])
        self.assertEqual(expected_result[9], self.test_result[9])
        self.assertEqual(expected_result[10], self.test_result[10])
        self.assertEqual(expected_result[11], self.test_result[11])
        self.assertEqual(expected_result[12], self.test_result[12])
        self.assertEqual(expected_result[13], self.test_result[13])


    def test_end_to_end(self):
        filepath = os.path.join('.', 'test.csv')
        folderpath = './test_created_files'
        value_1 = 'City'
        value_2 = 'Country'
        filepath_1 = os.path.join(folderpath, '{0}.csv'.format(value_1))
        filepath_2 = os.path.join(folderpath, '{0}.csv'.format(value_2))
        expected_result_file_1 = 'Number,Greeting,Target Type,Name,Status\n42,"Hello,World",City,Bar0,Active\n1,"Hello,World",City,Bar3,Active\n9003,Hi,City,Bar8,Active\n9004,Hi,City,Bar11,Active\n'
        expected_result_file_2 = 'Number,Greeting,Target Type,Name,Status\n67,Hey Man,Country,Bar9,Active\n525,Hey Man,Country,Bar12,Active\n5600,Hey Man,Country,Bar13,Active\n'
        expected_stdout = 'Printing the 10 most often repeated values from the "Name" column.\nBar0\nBar1\nBar2\nBar3\nBar4\nBar5\nBar6\nBar7\nBar8\nBar9\n'

        stdout_ = sys.stdout  # IMPORTANT: track of the current stdout
        stream = io.StringIO()
        sys.stdout = stream

        test_filter_fn = read_csv_to_list_of_ordered_dicts([aggregate_top_in_field([print_top_in_field([])]),
                                                           apply_column_filter([group_by_column([
                                                                                                reverse_field_order_in_ordered_dict_list(
                                                                                                        [save_to_output_directory_by_field(
                                                                                                                [],
                                                                                                                folderpath)])])])])
        test_filter_fn(filepath)

        sys.stdout = stdout_  # IMPORTANT: restore the previous stdout
        actual_stdout = stream.getvalue()


        actual_result_file_1 = self.get_contents_of_file(filepath_1)
        actual_result_file_2 = self.get_contents_of_file(filepath_2)

        self.assertEqual(expected_result_file_1, actual_result_file_1)
        self.assertEqual(expected_result_file_2, actual_result_file_2)
        self.assertEqual(expected_stdout, actual_stdout)

        # delete the test files
        for file in [filepath_1, filepath_2]:
            os.remove(file)


if __name__ == "__main__":
    unittest.main()