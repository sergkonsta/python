# Developer:    Sergey Konstantinovsky
# Date:         25/08/2020
# Home Assignment
#
#
# REPORTER FOR SINGLE SCENARIO
#
#
# PROVIDE ABSOLUTE PATH TO SCENARIO.csv FILE VIA ARGV[1]
# SCENARIO ID VIA ARGV[2]


import math     # for ceil
import sys  # for argv
import pandas as pd  # for read_csv
import sqlite3  # for sqlite database
from sqlite3 import Error
from fpdf import FPDF  # for pdf conversion

# below 20% won't be in the report
FAILURE_THRESHOLD = 20

# system requirements
REQ_COLUMN_LABELS = ['R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11', 'R12', 'R13', 'R14', 'R15',
                     'R16', 'R17', 'R18', 'R19', 'R20', 'R21', 'R22', 'R23', 'R24', 'R25', 'R26', 'R27', 'R28', 'R29',
                     'R30', 'R31', 'R32', 'R33', 'R34', 'R35', 'R36', 'R37', 'R38', 'R39']


def _CreateConnection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def _CreateTable(db_conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = db_conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def _InsertDataToDB(db_conn, data_to_insert):
    """
    insert new data into the results table
    :param conn:
    :param data_to_insert:
    :return: project id
    """
    sql = ''' INSERT INTO results(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16, R17, R18, R19, R20, R21, R22, R23, R24, R25, R26, R27, R28, R29, R30, R31, R32, R33, R34, R35, R36, R37, R38, R39)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = db_conn.cursor()
    cur.execute(sql, data_to_insert)
    db_conn.commit()
    return cur.lastrowid


def _CreateReport(failed_percentage):
    """
    creates pdf file with total failed percentage
    :param failed_percentage:
    :return: ---
    """

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="Report", ln=1, align='C')
    pdf.cell(200, 10, txt="Scenario ID: {0}".format(sys.argv[2]), ln=2, align='C')

    # prints failures above threshold to the report
    for i in range(39):
        if failed_percentage[i] > FAILURE_THRESHOLD:
            pdf.cell(200, 10, txt="Requirement {0}: {1}% failure".format(i + 1, failed_percentage[i]), ln=3)

    pdf.output("report.pdf")


def _AggTotalFailures(csv_file, failed_percentage):

    for y in range(39):

        amount_passed = (csv_file['{0}'.format(REQ_COLUMN_LABELS[y])] == 'PASS').sum()
        amount_failed = (csv_file['{0}'.format(REQ_COLUMN_LABELS[y])] == 'FAIL').sum()

        failed_percentage[y] = math.ceil(( amount_failed / (amount_failed + amount_passed + 1)) * 100)

    return failed_percentage


def main():
    database = 'test_results.db'
    results_table = """CREATE TABLE IF NOT EXISTS results (
                                                 R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16, 
                                                 R17, R18, R19, R20, R21, R22, R23, R24, R25, R26, R27, R28, R29, R30, 
                                                 R31, R32, R33, R34, R35, R36, R37, R38, R39
                                                 );"""
    failed_percentage = [None] * 39

    # open csv file in read mode
    file = pd.read_csv(sys.argv[1], low_memory=False)

    # create a database connection
    conn = _CreateConnection(database)

    # create tables
    _CreateTable(conn, results_table)

    # aggregations
    failed_percentage = _AggTotalFailures(file, failed_percentage)

    # save data to DB
    data_id = _InsertDataToDB(conn, failed_percentage)

    _CreateReport(failed_percentage)


if __name__ == '__main__':
    main()
