from clickhouse_driver import Client 
 
client = Client(host='localhost') 
 
client.execute(
    'INSERT INTO test (x) VALUES',
    [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
)
