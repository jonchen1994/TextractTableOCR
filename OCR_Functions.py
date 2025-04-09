# This code was largely and heavily adapated from materials found here: https://docs.aws.amazon.com/textract/
# Last udpated 2/20/2025

import boto3
# from textractor import Textractor
# from textractor.data.constants import TextractFeatures
import os

def get_table_csv_results(blocks):

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"

    
    csv_list = []
    for index, table in enumerate(table_blocks):
        csv = ''
        csv += generate_table_csv(table, blocks_map, index + 1)
        # csv += '\n\n'

        csv_list.append(csv)
        # In order to generate separate CSV file for every table, uncomment code below
        #inner_csv = ''
        #inner_csv += generate_table_csv(table, blocks_map, index + 1)
        #inner_csv += '\n\n'
        #output_file = file_name + "___" + str(index) + ".csv"
        # replace content
        #with open(output_file, "at") as fout:
        #    fout.write(inner_csv)

    return csv_list

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    # get cells.
    csv = ''

    for row_index, cols in rows.items():
        temp_str = ''
        for col_index, text in cols.items():
            temp_str += '{}'.format(text) + ";"

        if temp_str.replace(';', "") != '':
            csv += temp_str+'\n'

    if csv.replace(";", "") == '\n':
        return ""
    # csv += '\n\n\n'

    
    return csv

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                try:
                    cell = blocks_map[child_id]
                    if cell['BlockType'] == 'CELL':
                        row_index = cell['RowIndex']
                        col_index = cell['ColumnIndex']
                        if row_index not in rows:
                            # create new row
                            rows[row_index] = {}

                        # get the text value
                        rows[row_index][col_index] = get_text(cell, blocks_map)
                except KeyError:
                    # print("Error extracting Table data - {}:".format(KeyError))
                    pass
    return rows


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    try:
                        word = blocks_map[child_id]
                        if word['BlockType'] == 'WORD':
                            text += word['Text'] + ' '
                        if word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] == 'SELECTED':
                                text += 'X '
                    except KeyError:
                        # print("Error extracting Table data - {}:".format(KeyError))
                        do_nothing = ''

    text = text.replace(';','')
    return text