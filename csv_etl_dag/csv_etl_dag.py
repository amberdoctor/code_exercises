import sys
import csv
import os.path
from operator import itemgetter
from collections import OrderedDict



def aggregate_top_in_field(list_of_functions, name_of_field='Name'):
    def aggregate_top(list_of_dicts):
        name_count = {'fieldname':name_of_field, 'counts':{}}
        for row in list_of_dicts:
            value = row[name_of_field]
            if value in name_count['counts']:
                name_count["counts"][value] += 1
            else:
                name_count['counts'][value] = 1

        for func in list_of_functions:
            func(name_count)
    return aggregate_top


def print_top_in_field(list_of_functions, number_to_print=10):
    def print_top(dict_fieldname_counts):

        # TODO how to handle ties for nth place - business logic clarification not available - assumption made to not show additional tied names and use python's sort to handle order of who gets printed
        print('Printing the {0} most often repeated values from the "{1}" column.'.format(number_to_print,
                                                                                          dict_fieldname_counts[
                                                                                              'fieldname']))
        name_count_list = dict_fieldname_counts['counts'].items()
        top_names = sorted(name_count_list, key=itemgetter(1), reverse=True)[:number_to_print]
        for name_count in top_names:
            print(name_count[0])
        # TODO how to handle too few names - business logic clarification not available - assumption make a print statement to indicate the shortage
        if len(top_names) < number_to_print:
            print('Notice: There were less names than the requested number to print.')
        for func in list_of_functions:
            func(dict_fieldname_counts)
    return print_top


def apply_column_filter(list_of_functions, column_name='Status', value='Active'):
    def apply_filter(data):
        new_data = list(filter(lambda x: x[column_name] == value, data))
        for func in list_of_functions:
            func(new_data)
    return apply_filter


def group_by_column(list_of_functions, column_name='Target Type'):
    def group_by(list_of_ordered_dicts):
        list_of_ordered_dicts_sorted_by_column = sorted(list_of_ordered_dicts, key=itemgetter(column_name))
        for func in list_of_functions:
            func(list_of_ordered_dicts_sorted_by_column)
    return group_by


def reverse_field_order_in_ordered_dict_list(list_of_functions):
    def reverse_ordered_dict(dict):
        return OrderedDict(reversed(list(dict.items())))

    def reverse_field_order(list_of_ordered_dicts):
        new_data = list(map(reverse_ordered_dict, list_of_ordered_dicts))
        for func in list_of_functions:
            func(new_data)
    return reverse_field_order


def save_to_output_directory_by_field(list_of_functions, folderpath, fieldname='Target Type'):
    def start_next_file(row):
        return ([row], row[fieldname])

    def write_file(current_fieldname, list_of_ordered_dicts):
        filename = '{0}.csv'.format(current_fieldname)
        filepath = os.path.join(folderpath, filename)
        fieldname_list = list_of_ordered_dicts[0].keys()
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldname_list)
            writer.writeheader()
            writer.writerows(list_of_ordered_dicts)

    def save_output(list_of_ordered_dicts_grouped_by_field):
        current_list = []
        current_fieldname = list_of_ordered_dicts_grouped_by_field[0][fieldname]
        for row in list_of_ordered_dicts_grouped_by_field:
            if row[fieldname] == current_fieldname:
                current_list.append(row)
            else:
                write_file(current_fieldname, current_list)
                current_list, current_fieldname = start_next_file(row)
        write_file(current_fieldname, current_list)
        for func in list_of_functions:
            func(list_of_ordered_dicts_grouped_by_field)
    return save_output


def read_csv_to_list_of_ordered_dicts(list_of_functions):
    def read_csv(csv_filepath_to_read):
        data = []
        with open(csv_filepath_to_read, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
        for func in list_of_functions:
            func(data)
    return read_csv



if __name__ == "__main__":
    filepath_to_read = sys.argv[1]  # read in the commandline param
    folderpath_to_write = sys.argv[2]  # read in the commandline param
    #TODO - more time required - create dsl and read in the topology from config
    my_csv_reader = read_csv_to_list_of_ordered_dicts([aggregate_top_in_field([print_top_in_field([])]),
                                                      apply_column_filter([group_by_column([reverse_field_order_in_ordered_dict_list([save_to_output_directory_by_field([], folderpath_to_write)])])])])
    my_csv_reader(filepath_to_read)

