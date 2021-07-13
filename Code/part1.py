import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import upload_toElastic

es = upload_toElastic.connect_elasticsearch()

dataframe = pd.read_csv("movies.csv")


# upload data to elastic
def upload_func():
    es.indices.create(index="movies", ignore=400)
    dataframe.insert(0, "ID", range(0, len(dataframe)))
    print("Working...")
    df_generator = upload_toElastic.df_generator(dataframe, 'movies')
    helpers.bulk(es, df_generator)
    print("Upload done!")


# Search data to elastic from user input
def user_request():
    user_input = input("Search Elasticsearch: ").strip()
    return user_input


# Take the user's parameters and put them into a Python
# dictionary structured like an Elasticsearch query
def set_query(user_req):
    query_body = {
        "query": {
            "multi_match": {
                "type": "best_fields",
                "query": user_req,
                "fields": []
            }
        }
    }
    return query_body


def get_results(result):
    all_hits = result['hits']['hits']  # take the result object and get the list of documents
    # inside its ["hits"]["hits"] dictionary

    for num, doc in enumerate(all_hits):  # iterate the nested dictionaries inside the ["hits"]["hits"] list
        print("\nResult ID: ", num)
        for key, value in doc.items():
            print(key, ": ", value)

    print("\nTotal hits: ", len(all_hits))


def print_menu():
    print("\n1)Upload movies.csv file to ElasticSearch\n"
          "2)Search in ElasticSearch\n"
          "3)Exit\n")


def get_menu_input():
    while True:
        user_in = input("Type menu option: ").strip()

        if user_in.isdigit() and 0 < int(user_in) < 4:
            return user_in

        else:
            print("Please type an integer between 1,2,3. ")


def run_menu():
    while True:
        print_menu()
        choice = get_menu_input()

        if choice == '1':
            upload_func()

        elif choice == '2':
            result = es.search(index="movies", body=set_query(user_request()), size=10000)  # return 10000 “hits”
            get_results(result)

        elif choice == '3':
            print("Bye!")
            break


run_menu()
