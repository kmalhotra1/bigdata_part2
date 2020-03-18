from datetime import datetime
from elasticsearch import Elasticsearch
from requests import get
from time import sleep
import json
from sodapy import Socrata


def create_and_update_index(index_name, doc_type):
    es = Elasticsearch()
    try:
        es.indices.create(index=index_name)
    except Exception:
        pass

    es.indices.put_mapping(
        index=index_name,
        doc_type=doc_type,
        body={
            doc_type: {
                "properties": {
                    "issue_date_time": {"type": "date"}, 
                    "issue_date": {"type": "text"},
                    "violation_time": {"type": "text"},
                    "fine_amount" : {"type": "integer"},
                    "penalty_amount" : {"type": "integer"},
                    "interest_amount" : {"type": "integer"},
                    "reduction_amount" : {"type": "integer"},
                    "payment_amount" : {"type": "integer"},
                    "amount_due" : {"type": "integer"},
                    "fine_amount" : {"type": "integer"}
                }
            }
        }
    )

    return es


def get_cars_data(API_KEY: str, page_size: int = 1000, num_pages: int = 1):

    dataset = 'nc67-uf89'
    datasource = "data.cityofnewyork.us"


    try:
        client = Socrata(datasource, API_KEY)

        if not num_pages:


            count_response = client.get(dataset, select = 'COUNT(*)')
            count = int(count_response[0].get('COUNT'))


            num_pages = round(count/page_size)
            page = 0

            while page < num_pages:

                if page == 0:
                    results = client.get(dataset, limit = page_size)
                else:
                    results += client.get(dataset, limit = page_size, offset = page * page_size)
                page += 1

        else:
            #total number = page_size * num_pages
            limit = page_size * num_pages
            results = client.get(dataset, limit = limit)

        return results

    except Exception as e:
            print(f"Something went wrong: {e}")
            raise




if __name__ == "__main__":

    # Step 1: create an elastic search "index" to store data
    es = create_and_update_index('opcv-index', 'cars')


    # Step 2: fetch cars data from the internets
    docks = get_cars_data("NCsVHjyZOC2quqvnWydS2qdXF", 1000, 10)

    # Step 3: Push data into the elastic search
    for dock in docks:
        
        if 'violation_time' not in dock:
            print("Violation time not found")
            continue

        try:

            dock['issue_date_time'] = datetime.strptime(dock['issue_date'] + " " + dock['violation_time'] + "M", '%m/%d/%Y %I:%M%p')
            #dock['issue_date_time'] = datetime.now()
            res = es.index(index='opcv-index', doc_type='cars', body=dock,)
            print(res['result'])
        
        except:
            print("Error")
            continue