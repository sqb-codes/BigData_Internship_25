# pip install happybase pydoop
import happybase
import pydoop.hdfs
from pydoop import hadoop

def connect_to_hbase():
    connection = happybase.Connection("localhost")
    connection.open()
    return connection

def load_data_into_hbase(conn, table_name, data):
    # create table
    if table_name not in conn.tables():
        conn.create_table(
            table_name, {
                'cf': dict()
            }
        )

    table = conn.table(table_name)
    for row in data:
        table.put(row[0], {'cf:column_name':row[1]})


def mapreduce_job(input_path, output_path):
    def mapper(context):
        for line in context.stdin:
            context.emit(line.strip(), 1)
    
    def reducer(context):
        current_key = None
        current_count = 0
        for key, count in context:
            if key != current_key:
                if current_key:
                    context.emit(current_key, current_count)
                current_key = key
                current_count = 0
                current_count += count
        if current_key:
            context.emit(current_key, current_count)
    
    job = hadoop.Job(input_path=input_path, output_path=output_path,
                     mapper=mapper, reducer=reducer)
    job.run()


def store_to_hdfs(output_path, file_path):
    hdfs = pydoop.hdfs
    with hdfs.open(output_path, 'w') as file:
        with open(file_path, 'r') as local_file:
            file.write(local_file.read())


def main():
    # Define the connection and table
    # Load some dummy data int HBase
    # Define input and output for MapReduce
    # Run map reduce job
    # Store processed data into HDFS
    pass
