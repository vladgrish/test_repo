import boto3
import psycopg2
import io
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client('s3', endpoint_url='http://localstack:4566')
bucket_name = "test-bucket"
file_name = "test_file_nasdaq.csv"


def upload_file():
    s3.upload_file("/home/user/git/test_repo/data/test_file_nasdaq.csv",
                   bucket_name, file_name)


def read_chunk(bucket_name, file_name, chunk_start, chunk_end):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name,
                        Range='bytes={}-{}'.format(chunk_start, chunk_end))
    return obj['Body'].read().decode('utf-8')


def save_to_db(conn, content):
    cursor = conn.cursor()
    f = io.StringIO(content)
    cursor.copy_from(f, 'nasdaq', sep=',')
    conn.commit()


def create_table(conn, header):
    cursor = conn.cursor()
    columns = header.strip().split(',')
    column_definitions = ',\n'.join([f'{col} text' for col in columns])
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS your_table_name (
        {column_definitions}
    );
    '''
    cursor.execute(create_table_sql)
    conn.commit()


def read_file_and_upload(bucket_name, file_name, chunk_size=1048576):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_size = obj['ContentLength']
    chunks = [(i, i+chunk_size-1) for i in range(0, file_size, chunk_size)]
    chunks[-1] = (chunks[-1][0], file_size-1)

    conn = psycopg2.connect(
        host="postgres",
        database="default",
        user="postgres",
        password="postgress"
    )

    header = read_chunk(bucket_name, file_name, 0, chunk_size).split('\n')[0]
    create_table(conn, header)

    with ThreadPoolExecutor() as executor:
        results = [executor.submit(
            read_chunk, bucket_name, file_name, start, end) for start, end, in chunks[1:]]

    with ThreadPoolExecutor() as executor:
        [executor.submit(save_to_db, conn, r.result()) for r in results]

    conn.close()
