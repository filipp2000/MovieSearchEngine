from elasticsearch import Elasticsearch


def connect_elasticsearch():
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    if es.ping():
        print('Connected...')
    else:
        print('It is not connected!')
    return es

# upload df to elastic
def df_generator(df, index_name):
    df_iter = df.iterrows()
    for index, line in df_iter:
        try:
            yield {
                "_index": index_name,
                "_type": "_doc",
                "_id": f"{line['ID']}",
                "_source": line.to_dict(),
            }
        except StopIteration:
            return

