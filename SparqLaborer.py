#!/usr/bin/python3

from __future__ import print_function
import inspect
import argparse
import imp
import csv
import json
import logging
import sys
import time
import os
import regex
import collections
from httplib2 import Http
import xlsxwriter
from pathlib import Path
from SPARQLWrapper import CSV, TSV, XML, JSON, SPARQLExceptions, SPARQLWrapper
from googleapiclient import discovery
from oauth2client import client, tools, file
from oauth2client.client import GoogleCredentials
# from SPARQLWrapper import SPARQLExceptions

def main():

    # argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", help="runs all queries in the specified file. (To create a template for such a file, use '-t'.)")
    parser.add_argument("-s", help="reads in a provided client_secret json file. If no client_secret.json is provided as argument, SparqLaborer will search the current folder for one. (A client_secret can be obtained by logging into the Google Developer Console where a projects needs to be registered.)")
    parser.add_argument("-c", help="reads in a provided credentials json file. If no credentials.json is provided as argument, SparqLaborer will search the current folder for one. If there does not exist a credentials file yet, you can create one by providing a client_secret, after which you should be directed to a google-login, the resulting credentials file will be saved in the current folder.")
    parser.add_argument("-t", action='store_true', help="creates a template file for showcasing the queries-layout")

    if len(sys.argv) == 1:
        print("\nERROR: No arguments given!")
        parser.print_help()
        sys.exit()

    args = parser.parse_args()



    # user wants to run a queries file and does not want to create a template file
    if args.r and not args.t:

        logging.basicConfig(filename="SparqLaborer.log", filemode="w", level=logging.INFO)

        # read queries collection file
        query_collection_module = imp.load_source('conf', args.r)

        # extract and validate data from the queries collection file
        query_collection_data_object = read_query_collection_data_input(query_collection_module, args.r)

        ## google authentication cases

        # case: user provides credentials.json
        if args.c:
            credentials_path = args.c
            client_secret_path = False

        # case: user provides client_secret.json
        elif args.s:
            client_secret_path = args.s
            credentials_path = False

        # case: user did not provide any file. Search local folder for files and load them
        else:
            files_list = os.listdir('./')

            # case: found credentials.json
            if "credentials.json" in files_list:
                credentials_path = "credentials.json"
                client_secret_path = False

            # case: found client_secret.json
            elif "client_secret.json" in files_list:
                client_secret_path = "client_secret.json"
                credentials_path = False

            # case: did not find either
            else:
                credentials_path = False
                client_secret_path = False


        # credentials for google api

        query_collection_data_object.credentials_path = credentials_path
        query_collection_data_object.client_secret_path = client_secret_path


        # save original state of queries-list, since meta_functions could change it which then
        # could interfere with multi-value iterations.
        queries_original_state = query_collection_module.queries.copy()

        has_next = True

        while has_next:

            # output_writer setup
            query_collection_data_object.output_writer = Output_writer(query_collection_data_object)

            # execute queries, get results with further query data returned
            execute_queries(query_collection_data_object)

            # pass results to custom post processing method in the query collection file (if present)
            if hasattr(query_collection_module, "custom_post_processing"):
                query_collection_module.custom_post_processing(query_collection_data_object)

            # Close xlsx writer
            query_collection_data_object.output_writer.close()

            has_next = query_collection_data_object.has_next()
            if has_next:

                # reset the queries list to its initial state
                query_collection_module.queries = queries_original_state





    # user wants to create a template file and does not run a queries-file
    elif args.t and not args.r:

        create_template()



    # invalid arguments, print help
    else:
        print("\nERROR: Invalid arguments!")
        parser.print_help()
        sys.exit()



def read_query_collection_data_input(query_collection_module, query_collection_filename):
    """Reads input from query collection file and convert into usable data structure available throughout the entire program execution"""

    # Since when they are accessed the current multi_value variable affects the read-out
    # in such a way that only the current value and not the whole list of values is returned
    # which is desired when the values are read for processing, but not desired for logging

    query_collection_data_object = Query_collection_data_object()

    query_collection_data_object.query_collection_module = query_collection_module
    query_collection_data_object.query_collection_filename = query_collection_filename
    query_collection_data_object.timestamp_start = time.strftime('%y%m%d_%H%M%S')
    message = \
        "\n\n################################\n" + \
        "Reading query collection file: " + query_collection_filename + "\n" + \
        "\ntimestamp: " + str(query_collection_data_object.timestamp_start)
    logging.info(message)
    print(message)


    # title

    logging.info("Reading title")
    try:
        query_collection_data_object.title = query_collection_module.title
        logging.info("title: " + str(query_collection_data_object._title))
    except AttributeError:
        query_collection_data_object.title = query_collection_data_object.timestamp_start
        message = "Did not find title in query collection file; using timestamp instead: " + \
                  query_collection_data_object.title + "."
        logging.info(message)
        print(message)


    # description

    logging.info("Reading description")
    try:
        query_collection_data_object.description = query_collection_module.description
        logging.info("description: " + str(query_collection_data_object._description))
    except AttributeError:
        message = "Did not find description in query collection file; ignoring."
        logging.info(message)
        query_collection_data_object.description = ""


    # output_destination

    logging.info("Reading output_destination")
    try:
        query_collection_data_object.output_destination = query_collection_module.output_destination
        logging.info("output_destination: " + str(query_collection_data_object._output_destination))
    except AttributeError:
        message = "Did not find output_destination in query collection file; using local folder instead."
        logging.info(message)
        print(message)
        query_collection_data_object.output_destination = "."


    # output_format

    logging.info("Reading output_format")
    try:
        query_collection_data_object.output_format = query_collection_module.output_format
        logging.info("output_format: " + str(query_collection_data_object._output_format))
    except AttributeError:
        message = "Did not find output_format in query collection file; using csv instead."
        logging.info(message)
        print(message)
        query_collection_data_object.output_format = CSV


    # summary_sample_limit

    logging.info("Reading summary_sample_limit")
    try:
        query_collection_data_object.summary_sample_limit = query_collection_module.summary_sample_limit
        logging.info("summary_sample_limit: " + str(query_collection_data_object._summary_sample_limit))
    except AttributeError:
        message = "Did not find valid summary_sample_limit in query collection file; assuming a limit of 5."
        logging.info(message)
        print(message)
        query_collection_data_object.summary_sample_limit = 5


    # cooldown_between_queries

    logging.info("Reading cooldown_between_queries")
    try:
        query_collection_data_object.cooldown_between_queries = query_collection_module.cooldown_between_queries
        logging.info("cooldown_between_queries: " + str(query_collection_data_object._cooldown_between_queries))
    except AttributeError:
        message = "Did not find cooldown_between_queries in query collection file; assuming zero instead."
        logging.info(message)
        print(message)
        query_collection_data_object.cooldown_between_queries = 0


    # write_empty_results

    logging.info("Reading write_empty_results")
    try:
        query_collection_data_object.write_empty_results = query_collection_module.write_empty_results
        logging.info("write_empty_results: " + str(query_collection_data_object._write_empty_results))
    except AttributeError:
        message = "Did not find write_empty_results in query collection file; assuming False instead."
        logging.info(message)
        print(message)
        query_collection_data_object.write_empty_results = False


    # count_the_results

    logging.info("Reading count_the_results")
    try:
        query_collection_data_object.count_the_results = query_collection_module.count_the_results
        logging.info("count_the_results: " + str(query_collection_data_object._count_the_results))
    except AttributeError:
        message = "Did not find count_the_results in query collcetion file; assuming results should be counted."
        logging.info(message)
        print(message)
        query_collection_data_object.count_the_results = True


    # endpoint

    logging.info("Reading endpoints")
    try:
        query_collection_data_object.endpoint = query_collection_module.endpoint
        logging.info("endpoint: " + str(query_collection_data_object._endpoint) )
    except AttributeError:
        message = "\nERROR: INVALID INPUT! Did not find endpoint in query collection file!"
        logging.error(message)
        sys.exit(message)


    return query_collection_data_object


def read_query_data_input(query_conf_module, query_collection_data_object):

    query_data_object = Query_data_object( query_collection_data_object=query_collection_data_object )
    logging.info("\n\n################################\nReading in next query\n")

    # title

    logging.info("Reading title of query")
    try:
        query_data_object.title = query_conf_module['title']
        logging.info("query title: " + str(query_data_object._title))
    except KeyError:
        logging.info("No title for query found; ignoring.")
        query_data_object.title = ""


    # description

    logging.info("Reading description of query")
    try:
        query_data_object.description = query_conf_module['description']
        logging.info("query description: " + str(query_data_object._description))
    except KeyError:
        logging.info("No description for query found; ignoring.")
        query_data_object.description = ""


    # query

    logging.info("Reading query of query")
    try:
        query_data_object.query = query_conf_module['query']
        logging.info("sparql query: " + str(query_data_object._query))
    except KeyError:
        message = "ERROR: No query found in an element of list 'queries'!"
        logging.error(message)
        sys.exit(message)


    # custom_data_container

    logging.info("Reading custom_data_container of query")
    try:
        query_data_object.custom_data_container = query_conf_module['custom_data_container']
        logging.info("custom_data_container: " + str(query_data_object.custom_data_container))
    except KeyError:
        logging.info("No custom_data_container found; ignoring.")
        query_data_object.custom_data_container = None


    # custom_meta_function

    logging.info("Reading custom_meta_function of query")
    try:
        query_data_object.custom_meta_function = query_conf_module['custom_meta_function']
        logging.info("custom_meta_function: " + str(query_data_object.custom_meta_function))
    except KeyError:
        logging.info("No custom_meta_function found; ignoring.")
        query_data_object.custom_meta_function = None


    return query_data_object




def execute_queries(query_collection_data_object):
    """Executes all the queries and calls the writer-method from the Output_writer object to write to its specified destinations"""

    def main(query_collection_data_object):

        message = \
            "\n\n################################\n" + \
            "Starting execution of query collection: " + query_collection_data_object.title + "\n" + \
            "\nsparql endpoint: " + query_collection_data_object.endpoint
        logging.info(message)
        print(message)



        # Before executing queries, Get the count of all triples in whole triplestore

        try:

            message = "Getting count of all triples in whole triplestore"
            logging.info(message)
            print(message)

            results, execution_duration = execute_query(
                "SELECT COUNT(*) WHERE {[][][]}", query_collection_data_object.endpoint, JSON)

            query_collection_data_object.count_triples_in_endpoint = \
                results["results"]["bindings"][0]["callret-0"]["value"]

            message = "count_triples_in_endpoint: " + query_collection_data_object.count_triples_in_endpoint
            logging.info(message)
            print(message)

            query_collection_data_object.header_error_message = None

        except Exception as ex:
            message = "EXCEPTION OCCURED! " + str(ex)
            print(message)
            logging.error(message)
            query_collection_data_object.header_error_message = message


        # Write header

        query_collection_data_object.output_writer.write_header_summary(query_collection_data_object)


        # execute queries

        query_id = 0
        query_collection_data_object.queries = []


        # Iterate over queries list in the originating python module (not over any list from a parsed data object!)
        #
        # Since the usage of meta_function on queries requires their logic to be implemented where the queries are written
        # , i.e. the "query collection file" written by end-users, SparqLaborer must iterate over the list object
        # within the according python module, contrary to reading this list into a data object and handling it all
        # in SparqLaborer (where meta_functions could not write to). Hence this iteration goes over the original list object.

        for query_conf_module in query_collection_data_object.query_collection_module.queries:


            # read in current query, constuct data object from it.
            query_data_object = read_query_data_input(query_conf_module, query_collection_data_object)
            query_collection_data_object.queries.append(query_data_object)


            # read information from data object used for logging and printout

            query_id += 1
            query_data_object.id = query_id

            message = \
                "\n\n################################\nExecute\n" + \
                "\nid: " + str(query_id) + \
                "\nTitle: " + query_data_object.title + \
                "\nDescription: " + query_data_object.description + \
                "\nQuery:\n" + query_data_object.query
            logging.info(message)
            print(message)


            # execute query and query for counting the results

            startTime = time.time()

            try:

                # execute query

                if query_collection_data_object.output_format == "XLSX":
                    output_format = CSV
                else:
                    output_format = query_collection_data_object.output_format

                results, execution_duration = execute_query(
                    query_data_object.query, query_collection_data_object.endpoint, output_format )
                query_data_object.results_raw = results
                query_data_object.results_execution_duration = execution_duration


                # execute query for counting results (if needs to be done)

                query_data_object.query_for_count = None
                query_data_object.results_lines_count = None

                if query_collection_data_object.count_the_results:

                    # For this, search for the first select statement, and replace it with
                    # select count(*) where ... and add a '}' to the end, to make the original select
                    # a sub-query
                    #
                    # This requires a non-standard regex module to use variable length negative look behind
                    # which are needed to detect only 'select' statements, where there are no '#'
                    # before, which would make it a comment and thus not necesseray to replace

                    logging.info("Creating query for counting results.")

                    pattern = regex.compile(r"(?<!#.*)select", regex.IGNORECASE | regex.MULTILINE)
                    query_for_count = pattern.sub(
                        "SELECT COUNT(*) WHERE { \nSELECT",
                        query_data_object.query,
                        1)
                    query_for_count += "\n}"

                    query_data_object.query_for_count = query_for_count

                    results_lines_count, execution_duration = execute_query(
                        query_for_count, query_collection_data_object.endpoint, JSON )

                    results_lines_count = results_lines_count["results"]["bindings"][0]["callret-0"]["value"]
                    query_data_object.results_lines_count = results_lines_count
                    logging.info("results_lines_count: " + query_data_object.results_lines_count + "\n")


            except SPARQLExceptions.SPARQLWrapperException as ex:
                message = "EXCEPTION OCCURED WHEN EXECUTING QUERY: " + str(ex) + "\n Continue with execution of next query."
                print(message)
                logging.error(message)
                query_data_object.error_message = str(ex)
                query_data_object.results_execution_duration = time.time() - startTime
                query_data_object.results_raw = None


            message = "\nEXECUTION FINISHED\nElapsed time: " + str(query_data_object.results_execution_duration)
            logging.info(message)
            print(message)


            # harmonize results for other uses later

            logging.info("harmonizing results")

            if query_data_object.results_raw is None:
                query_data_object.results_matrix = [[query_data_object.error_message]]

            else:
                query_data_object.results_matrix = get_harmonized_result(
                    query_data_object.results_raw, query_collection_data_object.output_format)

            logging.info("Done with harmonizing results")


            # write results

            query_collection_data_object.output_writer.write_query_summary(query_data_object)
            query_collection_data_object.output_writer.write_query_result(query_data_object)


            # run custom meta function (if present)

            query_data_object.call_custom_meta_function()


            # cooldown between query-runs to prevent google api exhaustion

            cooldown = query_collection_data_object.cooldown_between_queries
            number_queries =  len(query_collection_data_object.query_collection_module.queries)
            if cooldown > 0 and query_id < number_queries:

                print("\nSleep for " + str(query_collection_data_object.cooldown_between_queries) + " seconds.")
                time.sleep(query_collection_data_object.cooldown_between_queries)


            # done with executing query; add its data_object to the collection_data_object

            query_collection_data_object.queries.append(query_data_object)



    def execute_query( query_string, endpoint, results_format ):
        """executes a query provided as string and returns the results in the asked-for format.
        Also returns duration of execution"""

        logging.info("Executing query: " + query_string)

        # Currently onyl accepts formats: CSV, TSV, XML, JSON
        # Other formats such as rdf-xml, turtle, and n-triples could be possible with a bit of tweaking.
        # Problems encountered so far are summarized here:
        # https://github.com/RDFLib/sparqlwrapper/issues/107
        sparql_wrapper = SPARQLWrapper(endpoint)
        sparql_wrapper.setQuery( query_string )
        sparql_wrapper.setReturnFormat( results_format )

        startTime = time.time()
        results = sparql_wrapper.query().convert()
        execution_duration = time.time() - startTime

        return results, execution_duration


    def get_harmonized_result(result, format):
        """Transforms the result data from its varying data formats into a two-dimensional list, used for writing summaries or into xlsx / google sheets files"""

        def get_harmonized_rows_from_keyed_rows(result_sample_keyed):
            """Some output formats require an intermediate step where the individual result rows are initially indexed by keys and their layout might change from row to row. Thus this methods iterate over each key-value row and transforms it into a regular two-dimensional list, where every column is identifiable by the same column-key"""

            # transform the result_sample_keyed into a regular two-dimensional list used for later inserting into xlsx or google sheets
            harmonized_rows = []
            harmonized_rows.append(result_sample_keyed[0])

            for y in range(1, len(result_sample_keyed)):

                sample_row = []
                for x in range(0, len(harmonized_rows[0])):
                    key = harmonized_rows[0][x]
                    sample_row.append(result_sample_keyed[y][key])

                harmonized_rows.append(sample_row)

            return harmonized_rows


        harmonized_result = []

        if result is None:
            return None
        else:


            # CSV, TSV, XLSX (since XLSX means CSV is internally used for querying the endpoint)

            if format == CSV or format == TSV or format == "XLSX":

                result = result.decode('utf-8').splitlines()
                harmonized_result = []
                valid_row_length = float("inf")

                if format == TSV:
                    reader = csv.reader(result, delimiter="\t")
                else:
                    reader = csv.reader(result)

                for row in reader:

                    row_harmonized = []

                    for column in row:

                        # check if value could be integer, if so change type
                        try:
                            column = int(column)
                        except ValueError:
                            pass

                        row_harmonized.append(column)

                    harmonized_result.append(row_harmonized)

                    # check validity of results
                    current_row_length = len(row)
                    if valid_row_length != float("inf") and valid_row_length != current_row_length:
                        message = "\nERROR: INVALID ROW LENGTH! " + str(row) + " has length " + str(current_row_length) + ", while valid length is " + str(valid_row_length)
                        logging.error(message)
                        sys.exit(message)
                    valid_row_length = current_row_length


            # JSON

            elif format == JSON:

                # construct list of dictionaries (to preserve the key-value pairing of individual row-results)

                result_keyed = []

                # get keys and save in first row of result_keyed
                keys = []
                for key in result.results_raw['bindings'][0]:
                    keys.append(key)
                result_keyed.append(keys)

                # go through the json - rows and extract key-value pairs from each, insert them into result_keyed
                valid_row_length = len(keys)
                for y in range(0, len(result.results_raw['bindings'])):
                    dict_tmp = {}

                    row = result.results_raw['bindings'][y]

                    for key in row:
                        column = row[key]['value']

                        # check if value could be integer, if so change type
                        try:
                            column = int(column)
                        except ValueError:
                            pass

                        dict_tmp[key] = column

                    # check validity of results
                    if len(row) != valid_row_length:
                        message = "\nERROR: INVALID ROW LENGTH! " + str(row) + " has length " + str(len(row)) + ", while valid length is " + str(valid_row_length)
                        logging.error(message)
                        sys.exit(message)

                    result_keyed.append(dict_tmp)

                harmonized_result = get_harmonized_rows_from_keyed_rows(result_keyed)


            # XML

            elif format == XML:

                # construct list of dictionaries (to preserve the key-value pairing of individual row-results)

                result_keyed = []

                # get keys and save in first row of result_keyed
                vars = result.getElementsByTagName("head")[0].getElementsByTagName("variable")
                keys = []
                for var in vars:
                    keys.append(var.getAttribute('name'))
                result_keyed.append(keys)

                # get results rows
                results = result.getElementsByTagName("result")

                # go through the xml results and extract key-value pairs from each, insert them into result_keyed
                valid_row_length = len(keys)
                for y in range(0, len(results)):

                    result = results[y]

                    dict_tmp = {}
                    for binding in result.getElementsByTagName("binding"):
                        # column = binding.childNodes[0].childNodes[0].nodeValue

                        child_node = binding.childNodes[0]

                        if child_node.localName == "literal" and child_node.childNodes == [] :
                            column = ""
                        else:
                            column = child_node.childNodes[0].nodeValue

                        # TODO: Check if this is best practice?
                        # check if value could be turned into an integer. If so change type, if not nothing happens
                        try:
                            column = int(column)
                        except ValueError:
                            pass

                        dict_tmp[binding.getAttribute('name')] = column

                    # check validity of results
                    if len(dict_tmp) != valid_row_length:
                        message = "\nERROR: INVALID ROW LENGTH! " + str(dict_tmp) + " has length " + str(len(dict_tmp)) + ", while valid length is " + str(valid_row_length)
                        logging.error(message)
                        sys.exit(message)

                    result_keyed.append(dict_tmp)

                harmonized_result = get_harmonized_rows_from_keyed_rows(result_keyed)

            return harmonized_result

    return main(query_collection_data_object)


def create_template():
    """Creates a template for the query collection file in the relative folder, where the script is executed"""

    template = """


# -------------------- OPTIONAL SETTINGS -------------------- 

# title
# defines the title of the whole set of queries
# OPTIONAL, if not set, timestamp will be used
title = \"TEST QUERIES\"


# description
# defines the textual and human-intended description of the purpose of these queries
# OPTIONAL, if not set, nothing will be used or displayed
description = \"This set of queries is used as a template for showcasing a valid query collection file.\"


# output_destination
# defines where to save the results, input can be: 
# * a local path to a folder 
# * a URL for a google sheets document  
# * a URL for a google folder
# NOTE: On windows, folders in a path use backslashes, in such a case it is mandatory to attach a 'r' in front of the quotes, e.g. r\"C:\\Users\\sresch\\..\"
# In the other cases the 'r' is simply ignored; thus best would be to always leave it there.
# OPTIONAL, if not set, folder of executed script will be used
output_destination = r\".\"


# output_format
# defines the format in which the result data shall be saved (currently available: csv, tsv, xml, json, xlsx)
# OPTIONAL, if not set, csv will be used
output_format = \"csv\"


# summary_sample_limit
# defines how many rows shall be displayed in the summary
# OPTIONAL, if not set, 5 will be used
summary_sample_limit = 3


# cooldown_between_queries
# defines how many seconds should be waited between execution of individual queries in order to prevent exhaustion of Google API due to too many writes per time-interval
# OPTIONAL, if not set, 0 will be used
cooldown_between_queries = 0


# write_empty_results
# Should tabs be created in a summary file for queries which did not return results? Possible values are python boolean values: True, False
# OPTIONAL, if not set, False will be used
write_empty_results = False


# -------------------- MANDATORY SETTINGS -------------------- 

# endpoint
# defines the SPARQL endpoint against which all the queries are run
# MANDATORY
endpoint = \"http://dbpedia.org/sparql\"

# queries
# defines the set of queries to be run. 
# MANDATAORY
queries = [
    {
        # title
        # OPTIONAL, if not set, timestamp will be used
        \"title\" : \"Optional title of first query\" ,

        # description
        # OPTIONAL, if not set, nothing will be used or displayed
        \"description\" : \"Optional description of first query, used to describe the purpose of the query.\" ,

        # query
        # the sparql query itself
        # NOTE: best practise is to attach a 'r' before the string so that python would not interpret some characters as metacharacters, e.g. \"\\n\"
        # MANDATORY
        \"query\" : r\"\"\"
            SELECT * WHERE {
                ?s ?p ?o
            }
            LIMIT 50
        \"\"\"
    },   
    {    
        \"title\" : \"Second query\" , 
        \"description\" : \"This query returns all triples which have a label associated\" , 
        \"query\" : r\"\"\"
            SELECT * WHERE {
                ?s <http://www.w3.org/2000/01/rdf-schema#label> ?o
            }
            LIMIT 50
        \"\"\"
    },
    {    
        \"query\" : r\"\"\"
            SELECT * WHERE {
                ?s ?p ?o . 
                FILTER ( ?p = <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> )
            }
            LIMIT 50
        \"\"\"
    },
]

# Each query is itself encoded as a python dictionary, and together these dictionaries are collected in a python list. 
# Beginner's note on such syntax as follows:
# * the set of queries is enclosed by '[' and ']'
# * individual queries are enclosed by '{' and '},'
# * All elements of a query (title, description, query) need to be defined using quotes as well as their contents, and both need to be separated by ':'
# * All elements of a query (title, description, query) need to be separated from each other using quotes ','
# * The content of a query needs to be defined using triple quotes, e.g. \"\"\" SELECT * WHERE .... \"\"\"
# * Any indentation (tabs or spaces) do not influence the queries-syntax, they are merely syntactic sugar.



# --------------- CUSTOM POST-PROCESSING METHOD --------------- 
'''
The method 'custom_post_processing(results)' is a stump for custom post processing which is always called if present and to which
result data from the query execution is passed. This way you can implement your own post-processing steps there.

The incoming 'results' argument is a list, where each list-element is a dictionary represting all data of a query.

This dictionary has the following keys and respective values:

* most likely to be needed are these two keys and values:
'query_title' - title of an individual query, as defined above.
'results_matrix' - the result data organized as a two dimensional list, where the first row contains the headers. 
This value is what you would most likely need to post process the result data.  

* other than these two, each query dictionary also contains data from and for SparqLaborer, which might be of use:
'query_description' - description of an individual query, as defined above.
'query_text' - the sparql query itself.
'results_execution_duration' - the duration it took to run the sparql query.
'results_lines_count' - the number of lines the sparql query produced at the triplestore.
'results_raw' - the result data in the specified format, encapsulated by its respective python class (e.g. a python json object).
'query_for_count' - an infered query from the original query, is used to get number of result lines at the triplestore.

As an example to print the raw data from the second query defined above, write:
print(results[1].results_matrix)
'''

# UNCOMMENT THE FOLLOWING LINES FOR A QUICKSTART:
'''    
def custom_post_processing(results):

    print(\"\\n\\Samples from the raw data:\\n\")

    for result in results:

        print(\"some results of query: \" + result.title)

        limit = 5 if len(result.results_matrix) > 5 else len(result.results_matrix)
        for i in range(0, limit):
            print(result.results_matrix[i])

        print()
'''
"""

    with open('template.py', 'w') as f:
        f.write(template)


class Output_writer:
    """the Output_writer Class encapsulates all technical details which vary due to the specified output destinations"""

    # general variables
    output_destination_type = None
    summary_sample_limit = None
    line_number = None

    # local folder and xlsx variables
    folder = None
    file_xlsx = None
    xlsx_workbook = None
    xlsx_worksheet_summary = None
    output_format = None
    bold_format = None
    title_2_format = None
    query_text_format = None

    # google folder and sheets variables
    google_service_sheets = None
    google_service_drive = None
    google_sheets_id = None
    google_sheets_summary_sheet_id = None

    def __init__(self, query_collection_data_object):

        def main():

            message = "\n\n################################\n" + \
            "Setting up output destination: " + query_collection_data_object.output_destination + "\n"
            logging.info(message)
            print(message)

            self.summary_sample_limit = query_collection_data_object.summary_sample_limit
            self.write_empty_results = query_collection_data_object.write_empty_results


            # output_destination_type, interpret from string

            if "google.com/drive/folders" in query_collection_data_object.output_destination:
                self.output_destination_type = "google_folder"
                logging.info("deduced output_destination_type: " + self.output_destination_type)
                init_google_folder()

            elif "google.com/spreadsheets" in query_collection_data_object.output_destination:
                self.output_destination_type = "google_sheets"
                logging.info("deduced output_destination_type: " + self.output_destination_type)
                init_google_sheets()

            elif query_collection_data_object.output_format == "XLSX" :
                self.output_destination_type = "local_xlsx"
                logging.info("deduced output_destination_type: " + self.output_destination_type)
                init_local_xlsx()

            else:
                self.output_destination_type = "local_folder"
                logging.info("deduced output_destination_type: " + self.output_destination_type)
                init_local_folder()


        def init_local_xlsx():
            """Creates a xlsx file in the respective folder"""

            # if locally saved, then "/" needs to be replaced with "-", since otherwise "/" would be interpreted as subfolder
            file_name = query_collection_data_object.title.replace("/", "-")

            # get or create folder for xlsx file
            self.folder = Path(str(query_collection_data_object.output_destination))
            self.folder.mkdir(parents=True, exist_ok=True)

            # create xlsx file
            self.file_xlsx = Path(
                self.folder / str(query_collection_data_object.timestamp_start + " - " + file_name + ".xlsx") )
            self.xlsx_workbook = xlsxwriter.Workbook(self.file_xlsx.open('wb'))
            self.xlsx_worksheet_summary = self.xlsx_workbook.add_worksheet("0. Summary")

            message = "Created local file: " + str(self.file_xlsx)
            logging.info(message)
            print(message)


        def init_local_folder():
            """Creates a folder (for the raw ouput) and a xlsx file (for the summary) in the respective folder"""

            # if locally saved, then "/" needs to be replaced with "-", since otherwise "/" would be interpreted as subfolder
            folder_name = query_collection_data_object.title.replace("/", "-")


            # create folder for queries and summary

            self.folder = Path(str(
                query_collection_data_object.output_destination + "/" +
                query_collection_data_object.timestamp_start + " - " +
                folder_name))

            self.folder.mkdir(parents=True, exist_ok=False)

            self.output_format = query_collection_data_object.output_format


            # Create xlsx file for summary

            self.file_xlsx = Path(self.folder / "0. Summary.xlsx")
            self.xlsx_workbook = xlsxwriter.Workbook(self.file_xlsx.open('wb'))
            self.xlsx_worksheet_summary = self.xlsx_workbook.add_worksheet("0. Summary")

            message = "Created local folder: " + str(self.folder)
            logging.info(message)
            print(message)


        def init_google_services():
            """Instantiates all necessary services for writing results to a specified google folder / sheets-file"""

            SCOPES = "https://www.googleapis.com/auth/drive"

            # Hardwired credentials
            #
            # !!! CAUTION !!!
            #
            # POSSIBILITY OF GRANTING FULL ACCESS TO YOUR PRIVATE GOOGLE DRIVE
            #
            # For ease of usage on your local machine, you can hardwire your credentials here
            # BUT ONLY DO THIS IF YOU NEVER SHARE THIS MODIFIED SCRIPT
            #
            # NEVER INSERT YOUR CREDENTIALS IF YOU WILL SHARE THIS SCRIPT!!


            # UNCOMMENT THE FOLLOWING LINES AND INSERT CONTENT OF CREDENTIALS.JSON FILE THERE
            # creds_hardcoded = json.loads("""
            #
            # """)

            # COMMENT OR DELETE THIS LINE
            creds_hardcoded = None


            # use credentials file if available

            if query_collection_data_object.credentials_path:
                creds = client.GoogleCredentials.from_json(open(query_collection_data_object.credentials_path).read())

            # if no credentials file is available, then create one using client_secret
            elif query_collection_data_object.client_secret_path:
                store = file.Storage('credentials.json')
                flow = client.flow_from_clientsecrets(query_collection_data_object.client_secret_path, SCOPES)

                # note: adding 'tools.argparser.parse_args(args=[])' here is important, otherwise
                # oauth2client.tools would parse the main command line arguments
                creds = tools.run_flow(flow, store, tools.argparser.parse_args(args=[]))

            elif creds_hardcoded:

                creds = GoogleCredentials(
                    creds_hardcoded['access_token'],
                    creds_hardcoded['client_id'],
                    creds_hardcoded['client_secret'],
                    creds_hardcoded['refresh_token'],
                    creds_hardcoded['token_expiry'],
                    creds_hardcoded['token_uri'],
                    creds_hardcoded['user_agent'],
                    creds_hardcoded['revoke_uri']
                )

            # if neither is available, abort
            else:
                message = "\nERROR: No client_secret.json or credentials.json provided nor found in local folder!."
                logging.error(message)
                sys.exit(message)

            # create services to be used by write functions
            if not creds.invalid:
                self.google_service_drive = discovery.build('drive', 'v3', http=creds.authorize(Http()))
                self.google_service_sheets = discovery.build('sheets', 'v4', http=creds.authorize(Http()))
            else:
                message = "\nERROR: Invalid credentials!"
                logging.error(message)
                sys.exit(message)


        def init_google_sheets():
            """Formats the give google sheets file, deletes old content and creates a summary-sheet"""

            init_google_services()


            # get id of google sheets file by extracting it from the url

            self.google_sheets_id = query_collection_data_object.output_destination\
                .split("docs.google.com/spreadsheets/d/",1)[1]\
                .split("/",1)[0]
            logging.info("ID of google sheets : " + str(self.google_sheets_id))

            # get list of existing sheets in sheets file
            google_sheets_metadata = self.google_service_sheets.spreadsheets().get(
                spreadsheetId=self.google_sheets_id).execute()
            all_sheet = google_sheets_metadata['sheets']


            # create new sheet reserved for summary

            body_create_summary_page = {
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "gridProperties": {
                                    "columnCount": 26
                                }
                            }
                        }
                    }
                ]
            }

            result = self.google_service_sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.google_sheets_id, body=body_create_summary_page).execute()
            self.google_sheets_summary_sheet_id = result['replies'][0]['addSheet']['properties']['sheetId']


            # delete all sheets except summary

            body_sheet_to_delete = { 'requests' : [] }
            for sheet in all_sheet:
                tmp = {
                    "deleteSheet": {
                        "sheetId": sheet['properties']['sheetId']
                    }
                }
                body_sheet_to_delete['requests'].append(tmp)

            self.google_service_sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.google_sheets_id, body=body_sheet_to_delete).execute()


            # rename summary sheet to '0. Summary'

            body_to_rename = {
                "requests" : [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": self.google_sheets_summary_sheet_id,
                                "title": "0. Summary",
                            },
                            "fields": "title",
                        }
                    },
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": self.google_sheets_summary_sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex":26
                            },
                            "properties": {
                                "pixelSize": 350
                            },
                            "fields": "pixelSize"
                        }
                    }
                ]
            }
            self.google_service_sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.google_sheets_id, body=body_to_rename).execute()


        # google folder
        def init_google_folder():
            """Creates a new google sheets file inside the specified google folder"""

            init_google_services()

            # get id of google folder by extracting it from the url
            self.google_folder_id = query_collection_data_object.output_destination\
                .split("drive.google.com/drive/folders/",1)[1]\
                .split("?",1)[0]
            logging.info("ID of google folder : " + str(self.google_folder_id))

            # Create google sheets file in folder
            body_spreadsheet = {
                'name': query_collection_data_object.timestamp_start + " - " + query_collection_data_object.title,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [self.google_folder_id]
            }
            sheets =  self.google_service_drive.files().create(body=body_spreadsheet).execute()
            self.google_sheets_id = sheets['id']
            self.google_sheets_summary_sheet_id = 0

            # Sets name of first sheet to summary, sets up column width
            body_to_rename = {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": self.google_sheets_summary_sheet_id,
                                "title": "0. Summary",
                            },
                            "fields": "title",
                        }
                    },
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": self.google_sheets_summary_sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex":26
                            },
                            "properties": {
                                "pixelSize": 300
                            },
                            "fields": "pixelSize"
                        }
                    }
                ]
            }
            self.google_service_sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.google_sheets_id, body=body_to_rename).execute()

            message = "Created google sheets at: " + "docs.google.com/spreadsheets/d/" + self.google_sheets_id
            logging.info(message)
            print(message)

        main()


    def write_header_summary(self, query_collection_data_object):
        """Writes the initial header to the summary sheet"""

        def main(query_collection_data_object):

            if self.output_destination_type == 'local_folder' or self.output_destination_type == 'local_xlsx':
                write_header_summary_xlsx_file(query_collection_data_object)

            elif self.output_destination_type == 'google_folder' or self.output_destination_type == 'google_sheets':
                write_header_summary_google_sheet(query_collection_data_object)


        def write_header_summary_xlsx_file(query_collection_data_object):
            """Writes header to xlsx file"""

            message = "Writing header to summary in local xslx"
            logging.info(message)
            print(message)

            # setup and formats
            self.xlsx_worksheet_summary.set_column('A:Z', 70)
            self.title_format = self.xlsx_workbook.add_format({'bold': True})
            self.title_format.set_font_size(16)
            self.title_2_format = self.xlsx_workbook.add_format({'bold': True})
            self.title_2_format.set_font_size(12)
            self.query_text_format = self.xlsx_workbook.add_format({'text_wrap': True})
            self.bold_format = self.xlsx_workbook.add_format({'bold': True})

            # Write header to xlsx
            self.xlsx_worksheet_summary.set_row(0, 20)
            self.line_number = 0
            self.xlsx_worksheet_summary.write(self.line_number, 0, query_collection_data_object.title, self.title_format)
            if query_collection_data_object.description is not None:
                self.xlsx_worksheet_summary.write(self.line_number + 1, 0, query_collection_data_object.description)
                self.line_number += 1
            self.line_number += 2
            self.xlsx_worksheet_summary.write(self.line_number, 0, "Execution timestamp of script: " + query_collection_data_object.timestamp_start)
            self.line_number += 1
            if query_collection_data_object.header_error_message is None:
                self.xlsx_worksheet_summary.write(self.line_number, 0, "Endpoint: " + query_collection_data_object.endpoint)
                self.line_number += 1
                self.xlsx_worksheet_summary.write(self.line_number, 0, "Total count of triples in endpoint: " + query_collection_data_object.count_triples_in_endpoint)
            else:
                self.xlsx_worksheet_summary.write(self.line_number, 0, query_collection_data_object.header_error_message)
            self.line_number += 4


        def write_header_summary_google_sheet(query_collection_data_object):
            """Writes header to google sheets file"""

            message = "Writing header to summary in google sheets"
            logging.info(message)
            print(message)

            # create header info
            self.line_number = 0
            header = []
            header.append([query_collection_data_object.title])
            if query_collection_data_object.description is not None:
                header.append([query_collection_data_object.description])
            header.append([])
            header.append(
                ["Execution timestamp of script: " +
                 query_collection_data_object.timestamp_start])
            if query_collection_data_object.header_error_message is None:
                header.append(["endpoint: " + query_collection_data_object.endpoint])
                header.append(
                    ["Total count of triples in endpoint: " +
                     query_collection_data_object.count_triples_in_endpoint])
            else:
                header.append([query_collection_data_object.header_error_message])


            # get range for header
            range = self.get_range_from_matrix(self.line_number, 0, header)
            range = "0. Summary!" + range
            self.line_number += len(header) + 3

            # write header to sheet
            self.google_service_sheets.spreadsheets().values().update(
                    spreadsheetId=self.google_sheets_id, range=range,
                    valueInputOption="RAW", body= { 'values': header } ).execute()

        main(query_collection_data_object)


    def write_query_result(self, query_data_object):
        """Writes results of query to the respective output destination"""

        def main(query_data_object):

            if len(query_data_object.results_matrix) > 1 or self.write_empty_results:
                message = "Writing results to output_destination"
                logging.info(message)
                print(message)

                if self.output_destination_type == 'local_xlsx':
                    write_query_result_to_xlsx_file(query_data_object)

                elif self.output_destination_type == 'local_folder':
                    write_query_result_to_local_folder(query_data_object)

                elif self.output_destination_type == 'google_sheets' or self.output_destination_type == 'google_folder':
                    write_query_result_to_google_sheets(query_data_object)


        def write_query_result_to_xlsx_file(query_data_object):
            """Writes results as harmonized two-dimensional list into a separate sheet in the xlsl file"""

            # create new worksheet and write into it
            sanitized_query_title = query_data_object.title\
                .replace("["," ") \
                .replace("]"," ") \
                .replace(":"," ") \
                .replace("*"," ") \
                .replace("?"," ") \
                .replace("/"," ") \
                .replace("\\"," ")

            sanitized_query_title = str(query_data_object.id) + sanitized_query_title

            if len(sanitized_query_title) > 30:
                sanitized_query_title = sanitized_query_title[:29]

            worksheet = self.xlsx_workbook.add_worksheet( sanitized_query_title )
            for y in range(0, len(query_data_object.results_matrix)):
                for x in range(0, len(query_data_object.results_matrix[y])):
                    column = query_data_object.results_matrix[y][x]
                    if len(str(column)) > 255:
                        column = str(column)[:255]
                    worksheet.write(y, x, column)


        def write_query_result_to_local_folder(query_data_object):
            """Writes raw output using the respective data format into the specified local folder"""


            # create file for query result
            # (and replace "/" with "-" because the file-writer interprets "/" as subdirectory)

            file_name = \
                str(query_data_object.id) + ". " + \
                query_data_object.title.replace("/", "-") + \
                "." + self.output_format
            local_file = Path(self.folder / file_name)


            ## differentiate between different result-types which require different write-methods

            # csv and tsv files need to be written as bytes
            if self.output_format == CSV or self.output_format == TSV:
                with local_file.open('wb') as fw:
                    fw.write(query_data_object.results_raw)

            # xml document is passed a writer object
            elif self.output_format == XML:
                with local_file.open('w') as fw:
                    query_data_object.results_raw.writexml(fw)

            # json needs json.dump() method
            elif self.output_format == JSON:
                with local_file.open('w') as fw:
                    json.dump(query_data_object.results_raw, fw)


        def write_query_result_to_google_sheets(query_data_object):
            """Writes results as harmonized two-dimensional list into a separate sheet in the google sheets file"""

            sanitized_query_title = query_data_object.title
            if len(sanitized_query_title) > 100:
                sanitized_query_title = sanitized_query_title[:99]

            sanitized_query_title = str(query_data_object.id) + ". " + sanitized_query_title

            # create sheet
            body_new_sheet = {
                'requests' : [
                    {
                        'addSheet': {
                            'properties': {
                                'title': sanitized_query_title,
                                'gridProperties': {
                                    'rowCount': len(query_data_object.results_matrix),
                                    'columnCount': len(query_data_object.results_matrix[0])
                                }
                            }
                        }
                    }
                ]
            }
            result = self.google_service_sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.google_sheets_id,
                body=body_new_sheet
            ).execute()
            google_sheet_id = result['replies'][0]['addSheet']['properties']['sheetId']
            body_change_columns = {
                'requests': [
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": google_sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 26
                            },
                            "properties": {
                                "pixelSize": 300
                            },
                            "fields": "pixelSize"
                        }
                    }
                ]
            }
            self.google_service_sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.google_sheets_id,
                body=body_change_columns
            ).execute()

            # get range of harmonized results
            google_sheet_range = \
                sanitized_query_title + "!" + \
                self.get_range_from_matrix(0, 0, query_data_object.results_matrix)

            # write into sheet
            self.google_service_sheets.spreadsheets().values().update(
                spreadsheetId=self.google_sheets_id,
                range=google_sheet_range,
                valueInputOption="RAW",
                body={ 'values': query_data_object.results_matrix}
            ).execute()

        main(query_data_object)


    def write_query_summary(self, query_data_object):
        """Writes the gist of the results of an executed query to a summary sheet"""

        def main(query_data_object):

            message = "Writing to summary"
            logging.info(message)
            print(message)

            if self.output_destination_type == 'local_xlsx' or self.output_destination_type == 'local_folder' :
                write_query_summary_xlsx_file(query_data_object)

            elif self.output_destination_type == 'google_sheets' or self.output_destination_type == 'google_folder':
                write_query_summary_google_sheets(query_data_object)


        def write_query_summary_xlsx_file(query_data_object):
            """Writes the gist of the results of an executed query to the summary sheet in the xlsx file"""

            # query_title
            self.xlsx_worksheet_summary.write(
                self.line_number,
                0,
                str(query_data_object.id) + ". " + str(query_data_object.title),
                self.title_2_format)
            self.line_number += 1

            # query description
            if not (query_data_object.description is None or
                    query_data_object.description.isspace() or
                    query_data_object.description == "") :

                self.xlsx_worksheet_summary.write(self.line_number, 0, query_data_object.description)
                self.line_number += 1

            # query_text
            size_of_query_text_row = 15 * (query_data_object.query.count("\n") + 2)
            self.xlsx_worksheet_summary.set_row(self.line_number, size_of_query_text_row)
            self.xlsx_worksheet_summary.write(self.line_number, 0, query_data_object.query, self.query_text_format)
            self.line_number += 1

            # results_execution_duration
            self.xlsx_worksheet_summary.write(self.line_number, 0, "Duration of execution in seconds: " + str(query_data_object.results_execution_duration))
            self.line_number += 1

            if query_data_object.results_raw is None:
                self.line_number += 1
                self.xlsx_worksheet_summary.write(self.line_number, 0, "NO RESULTS DUE TO ERROR: " + query_data_object.error_message)
                self.line_number += 1

            else:
                # results_lines_count
                if query_data_object.results_lines_count is not None:
                    self.xlsx_worksheet_summary.write(self.line_number, 0, "Total count of lines in results: " + str(query_data_object.results_lines_count))
                    self.line_number += 2
                else:
                    self.line_number += 1

                # results
                limit = self.summary_sample_limit
                if limit != 0:

                    self.xlsx_worksheet_summary.write(self.line_number, 0, "Sample results: ", self.bold_format)
                    self.line_number += 1
                    harmonized_rows = query_data_object.results_matrix

                    limit += 1
                    if len(harmonized_rows) < limit:
                        limit = len(harmonized_rows)

                    y = 0
                    for y in range(0, limit):
                        for x in range(0, len(harmonized_rows[y])):

                            column = harmonized_rows[y][x]

                            if len(str(column)) > 255:
                                column = str(column)[:255]
                            self.xlsx_worksheet_summary.write(y + self.line_number, x, column)

                    self.line_number += 1

                self.line_number += limit

            self.line_number += 2


        def write_query_summary_google_sheets(query_data_object):
            """Writes the gist of the results of an executed query to the summary sheet in the google sheets file"""

            # creating header
            query_stats = []
            query_stats.append([str(query_data_object.id) + ". " + query_data_object.title])
            if not (query_data_object.description.isspace() or query_data_object.description == "") :
                query_stats.append([query_data_object.description])
            query_stats.append([query_data_object.query])
            query_stats.append(
                ["Duration of execution in seconds: " +
                 str(query_data_object.results_execution_duration)])


            if query_data_object.results_raw is None:
                query_stats.append([])
                query_stats.append(["NO RESULTS DUE TO ERROR: " + query_data_object.error_message])

            else:

                if query_data_object.results_lines_count is not None:
                    query_stats.append(
                        ["Total count of lines in results: " +
                         str(query_data_object.results_lines_count)])

                # get sample results
                limit = self.summary_sample_limit
                if limit != 0:

                    query_stats.append([])
                    query_stats.append(["Sample results: "])
                    harmonized_rows = query_data_object.results_matrix

                    # set limit as defined, readjust if results should be less than it or if it exceeds gsheets-capacities
                    limit += 1
                    if len(harmonized_rows) < limit:
                        limit = len(harmonized_rows)

                    for y in range(0, limit):
                        query_stats.append(harmonized_rows[y])

            # write header and sample results to sheet
            google_sheet_range = self.get_range_from_matrix(self.line_number, 0, query_stats)
            google_sheet_range = "0. Summary!" + google_sheet_range
            self.line_number += len(query_stats) + 3

            self.google_service_sheets.spreadsheets().values().update(
                spreadsheetId=self.google_sheets_id,
                range=google_sheet_range,
                valueInputOption="RAW",
                body= { 'values': query_stats }
            ).execute()

        main(query_data_object)


    def get_range_from_matrix(self, start_y, start_x, matrix):
        """Input: starting y- and x-coordinates and a matrix.
        Output: Coordinates of the matrix (left upper cell and lower right cell) in A1-notation for updating google sheets"""

        max_len_x = 0
        for row in matrix:
            if len(row) > max_len_x:
                max_len_x = len(row)

        max_len_y = len(matrix)

        range_start = chr(64 + start_x + 1) + str(start_y + 1)

        range_end = chr(64 + start_x + max_len_x) + str(start_y + max_len_y)

        return range_start + ":" + range_end


    def close(self):
        """Closes the xlsx writer object"""

        if self.output_destination_type == "local_xlsx" or self.output_destination_type == 'local_folder' :
            logging.info("close writer")
            self.xlsx_workbook.close()




class Query_collection_data_object:
    """Data object encapsulating all data around a query collection file,
    while also providing some logic (especially regarding multi values)

    Attributes provided by end-user:
        title: title of the whole query collection (optional)
        description: description of the whole query collection (optional)
        output_destination: where to save the results (optional, default: current folder)
        output_format: what format should the results be saved in (optional, default: csv)
        summary_sample_limit: how many rows from the results should be used as sample (optional, default: 5)
        cooldown_between_queries: how many seconds should the execution be paused between queries (optional)
        count_the_results: should results of queries be counted (optional, default: yes)
        write_empty_results: should empty results be written into summaries (optional)
        endpoint: which sparql endpoint (mandatory)
        queries: the list containing query data objects
        credentials_path: path to google credentials (optional)
        client_secret_path: path to google client secrets (optional)

    Attributes handled by SparqLaborer internally:
        output_writer: object which handles all the output writing
        query_collection_module: the query collection file written by the end-user
        query_collection_filename: the original file name of the query collection file
        timestamp_start: start of execution
        count_triples_in_endpoint: how many triples are there in the store in total
        header_error_message: message if an error occured during counting of all triples
        current_multi_value: which current multi value index is used
        multi_value_length: how many multi value options are the maximumg



    """

    def __init__(self):

        # current_multi_value represents the current index the query_collection is in regard to the possible
        # multi values lists provided by the user
        # e.g. two output_formats: [xml, csv], where current_multi_value represents the current element in the list.
        # Is set to -1 because the method 'has_next' is called before the first item is accessed.
        self._current_multi_value = 0

        # multiv_value_length represents the possible length of a multi_value list.
        self._multi_value_length = 1


    # All the following variables could contain multi-values provided by the user,
    # Thus when being read from the query collection file, they need to parsed into proper lists if needed,
    # and when being used during processing, the correct current value of a multi-value list needs to be returned.


    # title

    @property
    def title(self):
        """Since the title is used as either file or folder or google sheet name, it's important for these
        names to be unique. Thus the following if's are checking for duplicate titles and if detected
        return them with a (n) attached where n represents the incrementing duplicate number.
        Note that this logic here does not actually persist the title in the query_collection_data_object,
        but only produces the adaptions on the fly when being queried."""

        if self._multi_value_length > 1 :

            if type(self._title) is list:

                found_count = 0
                for i in range(self._current_multi_value):
                    if self._title[i] ==  self._title[self._current_multi_value]:
                        found_count += 1

                if found_count > 0:
                    return self._title[self._current_multi_value] + " (" + str(found_count+1) + ")"
                else:
                    return self._title[self._current_multi_value]

            else:
                return self._title + " (" + str(self._current_multi_value+1) + ")"
        else:
            return self._title

    @title.setter
    def title(self, title):

        if type(title) is list:
            unsanitised_list = self.construct_multi_values(title)
            self._title = [ str(e) for e in unsanitised_list ]
        else:
            self._title = str(title)


    # description

    @property
    def description(self):
        return self.return_current_multi_value_of(self._description)

    @description.setter
    def description(self, description):

        def sanitise_description(unsanitised_description):

            if unsanitised_description is None:
                return ""
            elif type(unsanitised_description) is str:
                return unsanitised_description
            else:
                error_message = "Found invalid data type for description! \n" + \
                    "Expected type: str\nFound type: " + str(type(unsanitised_description)) + \
                    "\nFound value: " + str(unsanitised_description)
                logging.error(error_message)
                raise ValueError(error_message)


        if type(description) is list:
            unsanitised_list = self.construct_multi_values(description)
            self._description = [ sanitise_description(e) for e in unsanitised_list ]
        else:
            self._description = sanitise_description(description)


    # output_destination

    @property
    def output_destination(self):
        return self.return_current_multi_value_of(self._output_destination)

    @output_destination.setter
    def output_destination(self, output_destination):

        def sanitise_output_destination(unsanitised_output_destination):

            if unsanitised_output_destination is None:
                return "."
            elif type(unsanitised_output_destination) is str:
                if unsanitised_output_destination == "" or unsanitised_output_destination.isspace():
                    return "."
                else:
                    return unsanitised_output_destination
            else:
                error_message = "Found invalid data type for output_destination! \n" + \
                    "Expected type: str\nFound type: " + str(type(unsanitised_output_destination)) + \
                    "\nFound value: " + str(unsanitised_output_destination)
                logging.error(error_message)
                raise ValueError(error_message)


        if type(output_destination) is list:
            unsanitised_list = self.construct_multi_values(output_destination)
            self._output_destination = [ sanitise_output_destination(e) for e in unsanitised_list ]
        else:
            self._output_destination = sanitise_output_destination(output_destination)


    # output_format

    @property
    def output_format(self):
        return self.return_current_multi_value_of(self._output_format)

    @output_format.setter
    def output_format(self, output_format):

        def sanitise_output_format(unsanitised_output_format):

            if unsanitised_output_format is None or type(unsanitised_output_format) is not str:
                error_message = "No valid output_format found. Possible formats are: \n" + \
                                 "CSV, TSV, XML, JSON, XLSX\n" + \
                                 "Found format is " + str(unsanitised_output_format)
                logging.error(error_message)
                raise ValueError(error_message)

            if unsanitised_output_format.upper() == "CSV" or unsanitised_output_format.upper() == CSV:
                return CSV
            elif unsanitised_output_format.upper() == "TSV" or unsanitised_output_format.upper() == TSV:
                return TSV
            elif unsanitised_output_format.upper() == "XML" or unsanitised_output_format.upper() == XML:
                return XML
            # TODO: json not working, look into why
            # elif unsanitised_output_format.upper() == "JSON":
            #     return JSON
            elif unsanitised_output_format.upper() == "XLSX" or unsanitised_output_format.upper() == XLSX:
                return "XLSX"
            else:
                error_message = "No valid output_format found. Possible formats are: \n" + \
                                 "CSV, TSV, XML, XLSX\n" + \
                                 "Found format is " + str(unsanitised_output_format)
                logging.error(error_message)
                raise ValueError(error_message)


        if type(output_format) is list:
            unsanitised_list = self.construct_multi_values(output_format)
            self._output_format = [ sanitise_output_format(e) for e in unsanitised_list ]
        else:
            self._output_format = sanitise_output_format(output_format)


    # summary_sample_limit

    @property
    def summary_sample_limit(self):
        return self.return_current_multi_value_of(self._summary_sample_limit)

    @summary_sample_limit.setter
    def summary_sample_limit(self, summary_sample_limit):

        def sanitise_summary_sample_limit(unsanitised_summary_sample_limit):

            if unsanitised_summary_sample_limit is None or type(unsanitised_summary_sample_limit) is not int:
                error_message = "Found invalid data type for summary_sample_limit! \n" + \
                    "Expected type: int\nFound type: " + str(type(unsanitised_count_the_results)) + \
                    "\nFound value: " + str(unsanitised_count_the_results)
                logging.error(error_message)
                raise ValueError(error_message)

            elif unsanitised_summary_sample_limit > 101:
                message = "Found sample limit which is too high: " + str(unsanitised_summary_sample_limit) + \
                          ", replaced it with limit of 100"
                logging.info(message)
                print(message)
                return 101

            elif unsanitised_summary_sample_limit < 0:
                message = "Found invalid sample limit: " + str(unsanitised_summary_sample_limit) + \
                          ", replaced it with limit of 5"
                logging.info(message)
                print(message)
                return 5

            else:
                return unsanitised_summary_sample_limit


        if type(summary_sample_limit) is list:
            unsanitised_list = self.construct_multi_values(summary_sample_limit)
            self._summary_sample_limit = [sanitise_summary_sample_limit(e) for e in unsanitised_list]
        else:
            self._summary_sample_limit = sanitise_summary_sample_limit(summary_sample_limit)


    # cooldown_between_queries

    @property
    def cooldown_between_queries(self):
        return self.return_current_multi_value_of(self._cooldown_between_queries)

    @cooldown_between_queries.setter
    def cooldown_between_queries(self, cooldown_between_queries):

        def sanitise_cooldown_between_queries(unsanitised_cooldown_between_queries):

            if unsanitised_cooldown_between_queries is None or type(unsanitised_cooldown_between_queries) is not int:
                error_message = "Found invalid type of cooldown_between_queries.\n" + \
                    "Expected type: int\nFound type: " + str(type(unsanitised_cooldown_between_queries)) + \
                    "\nFound value: " + str(unsanitised_cooldown_between_queries)
                logging.error(error_message)
                raise ValueError(error_message)

            else:

                if unsanitised_cooldown_between_queries >= 0:
                    return unsanitised_cooldown_between_queries

                else:
                    error_message = "Found invalid value for cooldown_between_queries: " + \
                        "Expected value: greater than 0\n" + \
                        "Found value:" + str(unsanitised_cooldown_between_queries)
                    logging.error(error_message)
                    raise ValueError(error_message)


        if type(cooldown_between_queries) is list:
            unsanitised_list = self.construct_multi_values(cooldown_between_queries)
            self._cooldown_between_queries = [ sanitise_cooldown_between_queries(e) for e in unsanitised_list ]
        else:
            self._cooldown_between_queries = sanitise_cooldown_between_queries(cooldown_between_queries)


    # write_empty_results

    @property
    def write_empty_results(self):
        return self.return_current_multi_value_of(self._write_empty_results)

    @write_empty_results.setter
    def write_empty_results(self, write_empty_results):

        def sanitise_write_empty_results(unsanitised_write_empty_results):

            if unsanitised_write_empty_results is None or type(unsanitised_write_empty_results) is not bool:
                error_message = "Found invalid type of write_empty_results.\n" + \
                    "Expected type: bool\nFound type: " + str(type(unsanitised_write_empty_results)) + \
                    "\nFound value: " + str(unsanitised_write_empty_results)
                logging.error(error_message)
                raise ValueError(error_message)

            else:
                return unsanitised_write_empty_results


        if type(write_empty_results) is list:
            unsanitised_list = self.construct_multi_values(write_empty_results)
            self._write_empty_results = [ sanitise_write_empty_results(e) for e in unsanitised_list ]
        else:
            self._write_empty_results = sanitise_write_empty_results(write_empty_results)


    # count_the_results

    @property
    def count_the_results(self):
        return self.return_current_multi_value_of(self._count_the_results)

    @count_the_results.setter
    def count_the_results(self, count_the_results):

        def sanitise_count_the_results(unsanitised_count_the_results):

            if unsanitised_count_the_results is None or type(unsanitised_count_the_results) is not bool:
                error_message = "Found invalid type of count_the_results.\n" + \
                    "Expected type: bool\nFound type: " + str(type(unsanitised_count_the_results)) + \
                    "\nFound value: " + str(unsanitised_count_the_results)
                logging.error(error_message)
                raise ValueError(error_message)

            else:
                return unsanitised_count_the_results


        if type(count_the_results) is list:
            unsanitised_list = self.construct_multi_values(count_the_results)
            self._count_the_results = [ sanitise_count_the_results(e) for e in unsanitised_list ]
        else:
            self._count_the_results = sanitise_count_the_results(count_the_results)


    # endpoint

    @property
    def endpoint(self):
        return self.return_current_multi_value_of(self._endpoint)

    @endpoint.setter
    def endpoint(self, endpoint):

        def sanitise_endpoint(unsanitised_endpoint):

            if unsanitised_endpoint is None or type(unsanitised_endpoint) is not str:
                error_message = "Found invalid type of endpoint.\n" + \
                    "Expected type: str\nFound type: " + str(type(unsanitised_endpoint)) + \
                    "\nFound value: " + str(unsanitised_endpoint)
                logging.error(error_message)
                raise ValueError(error_message)

            else:
                return unsanitised_endpoint


        if type(endpoint) is list:
            unsanitised_list = self.construct_multi_values(endpoint)
            self._endpoint = [ sanitise_endpoint(e) for e in unsanitised_list ]
        else:
            self._endpoint = endpoint



    # Helper functions handling the multi-value lists (e.g. returning correct single value,
    # constructing a list out of multiple lists, checking if there is more to be handled)

    def return_current_multi_value_of(self, object_variable):
        """If object_variable is a multi-value list, then this method returns the correct element according
        to the current_multi_value index.
        If not, return object_variable as it is."""

        if type(object_variable) is list:
            return object_variable[self._current_multi_value]
        else:
            return object_variable


    def construct_multi_values(self, unprocessed_list):
        """Takes a list and analyzes its content, whether or not elements need to be concatenated
        which can happen in the example case that a multiv_value list contains a sublist of multi value elements
        and a static element which should be joined to every element from the sublist
        (E.G title: ["queries on datasource ", ["A","B"] ] -> ["queries on datasource A", "queries on datasource B"]"""

        processed_list = []

        # if any sublist in there, then all the values of the sublist must be concatenated with the other elements
        if any(type(v) is list for v in unprocessed_list):

            for sub_value in unprocessed_list:

                # if current sub value is list, then all its values must be concatenated to the value from before
                if type(sub_value) is list:

                    # there are no pre existing values, this sub list is first element, initiate results with it
                    if len(processed_list) == 0:
                        processed_list = sub_value

                    # one pre existing value, can only be simple value, now concatenate this value with every
                    # value of the sub list
                    elif len(processed_list) == 1:
                        processed_list = [str(processed_list[0]) + str(sub_value_element) for
                                         sub_value_element in
                                         sub_value]

                    # pre existing sub list, concatenate each value of the existing with the new values
                    # must have same length since it is meant for multi-values to be used pair-wise
                    elif len(processed_list) == len(sub_value):
                        for i in range(0, len(processed_list)):
                            processed_list[i] = str(processed_list[i]) + str(sub_value[i])

                    else:
                        raise ValueError(
                            "multiple multi-values and/or sublists do not have the same length!" + str(unprocessed_list))

                # current sub value is no list, simply concatenate that to whatever exists before
                else:

                    # no pre existing value, use current sub value as first element of results
                    if len(processed_list) == 0:
                        processed_list = [sub_value]

                    # there are pre existing values, concatenate to each of these the new sub value
                    else:
                        processed_list = [str(result_value) + str(sub_value) for result_value in
                                         processed_list]

        # no sublists, the main list is meant as a set of disjunctive values, return it as it is
        else:
            processed_list = unprocessed_list

        if self._multi_value_length > 1 and len(processed_list) != self._multi_value_length:
            raise ValueError("Some multi-value lists do not have the same length!\n" +
                             "\nCurrent list: " + str(processed_list) + \
                             "\nLength of current multi-value list: " + str(len(processed_list)) + \
                             "\nLength of other multi-value list: " + str(self._multi_value_length))
        else:
            self._multi_value_length = len(processed_list)

        return processed_list

    def has_next(self):
        """Returns a boolean value expressing whether or not there is more multi-values not yet read.
        Also increments the current multi-value index."""

        self._current_multi_value += 1
        return self._current_multi_value < self._multi_value_length




class Query_data_object:
    """Data object encapsulating all data around a query,
    while also providing some logic (especially regarding multi values)

    Attributes provided by end-user:
        title: query title (optional)
        description: query description (optional)
        query: the sparql query (mandatory)
        custom_meta_function: arbitrary python code included in the query collection file; if present will be executed
        custom_data_container: arbitrary data field which can be used in conjunction with a custom_meta_function

    Attributes handled by SparqLaborer internally:
        query_collection_data_object: the associated collection data object (important for multi value coordination)
        id: query data object id (mostly for logging, but also to provide identification for meta function calls)
        results_raw: the result from the sparql query, raw since they are saved as whatever data format was used for it
        results_matrix: the results converted into a matrix (first row: variables, all others: their values)
        results_execution_duration: the duration it took the query to be run until a result was returned (or an error)
        query_for_count: an automatically created query adapted from the base query, in order to count the results
        results_line_count: the total number of result lines from a given sparql query
        error_message: in case of an error encountered, the message will be saved and returned using this attribute

    """

    def __init__(self, query_collection_data_object):

        # mandatory attribute: the associated collection_data_object. Thus no default value (=None) assigned to it.
        self._query_collection_data_object = query_collection_data_object



    # title
    #
    # Overriden getters and setters are necessary here since they could contain multi values

    @property
    def title(self):
        return self._query_collection_data_object.return_current_multi_value_of( self._title )

    @title.setter
    def title(self, title):

        def sanitise_title(unsanitised_title):

            if unsanitised_title is None or type(unsanitised_title) is not str:
                error_message = "Found invalid type of title of query.\n" + \
                    "Expected type: str\nFound type: " + str(type(unsanitised_title)) + \
                    "\nFound value: " + str(unsanitised_title)
                logging.error(error_message)
                raise ValueError(error_message)
            else:
                return unsanitised_title


        if type(title) is list:
             unsanitised_list = self._query_collection_data_object.construct_multi_values( title )
             self._title = [ sanitise_title(e) for e in unsanitised_list ]
        else:
            self._title = sanitise_title(title)


    # description
    #
    # Overriden getters and setters are necessary here since they could contain multi values

    @property
    def description(self):
        return self._query_collection_data_object.return_current_multi_value_of( self._description )

    @description.setter
    def description(self, description):

        def sanitise_description(unsanitised_description):

            if unsanitised_description is None or type(unsanitised_description) is not str:
                error_message = "Found invalid type of description of query.\n" + \
                    "Expected type: str\nFound type: " + str(type(unsanitised_description)) + \
                    "\nFound value: " + str(unsanitised_description)
                logging.error(error_message)
                raise ValueError(error_message)
            else:
                return unsanitised_description


        if type(description) is list:
            unsanitised_list = self._query_collection_data_object.construct_multi_values( description )
            self._description = [ sanitise_description(e) for e in unsanitised_list ]
        else:
            self._description = description


    # query
    #
    # Overriden getters and setters are necessary here since they could contain multi values

    @property
    def query(self):
        return self._query_collection_data_object.return_current_multi_value_of( self._query )

    @query.setter
    def query(self, query):

        def scrub_query(query_text):
            """Scrubs the queries clean from unneccessary whitespaces and indentations, to prevent unneccessary indentations
            when including original sparql queries into the summaries."""

            if not query_text.isspace() and not query_text == "":

                # replace tabs with spaces for universal formatting
                query_lines = query_text.replace("\t", "    ").splitlines()

                # get smallest number of whitespaces in front of all lines
                spaces_in_front = float("inf")
                for j in range(0, len(query_lines)):

                    if not query_lines[j].isspace() and not query_lines[j] == "":

                        spaces_in_front_tmp = len(query_lines[j]) - len(query_lines[j].lstrip(" "))
                        if spaces_in_front_tmp < spaces_in_front:
                            spaces_in_front = spaces_in_front_tmp

                # remove redundant spaces in front
                if spaces_in_front > 0:
                    query_text = ""
                    for line in query_lines:
                        query_text += line[spaces_in_front:] + "\n"

                # remove "" and heading and unneccessary newlines
                query_lines = query_text.splitlines()
                query_text = ""
                for line in query_lines:
                    if not line.isspace() and not line == "":
                        query_text += line + "\n"

            return query_text


        def sanitise_query(unsanitised_query):

            if unsanitised_query is None or type(unsanitised_query) is not str:
                error_message = "Found invalid type of query.\n" + \
                    "Expected type: str\nFound type: " + str(type(unsanitised_query)) + \
                    "\nFound value: " + str(unsanitised_query)
                logging.error(error_message)
                raise ValueError(error_message)
            else:
                return scrub_query(unsanitised_query)




        if type(query) is list:
             unsanitised_list = self._query_collection_data_object.construct_multi_values( query )
             self._query = [ sanitise_query(e) for e in unsanitised_list ]
        else:
            self._query = query


    # custom_meta_function

    def call_custom_meta_function(self):
        """Calling the custom_meta_function if it is present."""

        logging.info("Calling custom_meta_function")

        if self.custom_meta_function is not None:

            message = "Found custom_meta_function; executing."
            logging.info(message)
            print(message)

            # check if meta_function has parameters, call it if it's the case
            signature = inspect.signature(self.custom_meta_function)
            if len(signature.parameters) == 0:
                self.custom_meta_function()
            elif len(signature.parameters) == 1:
                self.custom_meta_function(self)
            else:
                message = "\nERROR: Too many parameters of meta function!\n" \
                          "Only zero or one can be passed to custom meta function"
                logging.error(message)
                sys.exit(message)

        else:
            logging.info("No custom_meta_function found; ignoring.")




main()