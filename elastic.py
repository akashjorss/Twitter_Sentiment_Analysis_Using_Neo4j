from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Elastic:

    def __init__(self, cloud_id, username, password):
        self.es = Elasticsearch(cloud_id=cloud_id,
                                http_auth=(username, password))

    def load_stream_analytics(self, **args):
        """Wrapper to load simulation data"""
        return None

    def load_data(self, data, index):
        """
        to load array of tweets to elastic search
        :param data: (list of json objects)
        :param index: (string) name of the elastic search index to load data to
        :return: None
        """

        # to make the index if it doesn't exist
        self.es.index(index, data[0], id=0)

        # Bulk insert
        actions = [{
            "_index": index,
            "_type": "_doc",
            "_id": j,
            "_source": data[j]
        } for j in range(0, len(data))]
        helpers.bulk(self.es, actions)
        print("insert successful")
        self.es.indices.refresh(index=index)
        print("data loaded to elastic")

    def delete_data(self, index, id_range):
        """
        :param index: (string) elastic search index from which docs are to be deleted
        :param id_range: (int) id range to be deleted
        :return: None
        """
        # Bulk delete
        actions = [{
            "_op_type": 'delete',
            "_index": index,
            "_id": j,
        } for j in range(0, id_range)]
        helpers.bulk(self.es, actions)

    def clear_data(self, index):
        """
        :param index: (string) elastic search index to be cleared
        :return: None
        """
        self.es.delete_by_query(index, body={"query": {"match_all": {}}})
