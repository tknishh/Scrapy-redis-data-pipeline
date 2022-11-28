import json

import redis

class RedisClient:
    connection =  redis.Redis(host='redis-15339.c15.us-east-1-4.ec2.cloud.redislabs.com', port=15339, db=0)
    key = 'DATA-PIPELINE-KEY'
    def _convert_data_to_json(self, data):
        try:
            return json.dumps(data)
        except Exception as e:
            print(f'Failed to convert data into json with error: {e}')
            raise e
    def _convert_data_from_json(self, data):
        try:
            return json.loads(data)
        except Exception as e:
            print(f'Failed to convert data from json to dict with error: {e}')
            raise e
    def send_data_to_pipeline(self, data):
        data = self._convert_data_to_json(data)
        self.connection.lpush(self.key, data)
    def get_data_from_pipeline(self):
        try:
            data = self.connection.rpop(self.key)
            return self._convert_data_from_json(data)
        except Exception as e:
            print(f'Failed to get more data from pipeline with error: {e}')
            raise e