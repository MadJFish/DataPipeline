import os
import glob


def combine_files(output_file, input_files):
    clean_file(output_file)
    write_header(output_file, input_files)
    write_all_school_data(output_file, input_files)


def write_header(output_file, input_files):
    with open(output_file, 'w', encoding="cp437") as write_stream:
        for file_name in glob.glob(input_files):
            with open(file_name, encoding="cp437") as read_stream:
                header = 'school_name,' + next(read_stream)
                write_stream.write(header)
                return True


def clean_file(output_file):
    if os.path.exists(output_file):
        os.remove(output_file)


def write_all_school_data(output_file, input_files):
    with open(output_file, 'a', encoding="cp437") as write_stream:
        for file_name in glob.glob(input_files):
            school_name = file_name.split('\\')[1]
            print('school name: ', school_name)
            write_school_data(write_stream, file_name, school_name)


def write_school_data(write_stream, file_name, school_name):
    try:
        with open(file_name, encoding="cp437") as read_stream:
            header = 'school_name,' + next(read_stream)
            for line in read_stream:
                line_content = school_name + ',' + line
                print('current line is: {}', line_content)
                write_stream.write(line_content)
    except FileNotFoundError:
        return "Error: exception occurs while processing file %s" % file_name


APP_NAME = "combine_results"  # Any unique name works
INPUT_FILES = '1-wip-data/3_distance_classifier/*/*.csv'
OUTPUT_FILE = '1-wip-data/merged.csv'

combine_files(OUTPUT_FILE, INPUT_FILES)
