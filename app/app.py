from app.csv_uploader.read_write import read_file_and_upload, bucket_name, file_name

if __name__ == "__main__":
    read_file_and_upload(bucket_name, file_name)
