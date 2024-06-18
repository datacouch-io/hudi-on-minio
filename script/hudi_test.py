from minio import Minio
LOCAL_FILE_PATH = "vehicles.csv"
ACCESS_KEY = "admin"
SECRET_KEY = "password"
MINIO_API_HOST = "http://minioserver:9000"
MINIO_CLIENT = Minio("minioserver:9000", access_key=ACCESS_KEY,
                     secret_key=SECRET_KEY, secure=False)


found = MINIO_CLIENT.bucket_exists("huditest")
if not found:
    MINIO_CLIENT.make_bucket("huditest")
else:
    print("Bucket already exists")

MINIO_CLIENT.fput_object("huditest", "vehicles.csv", LOCAL_FILE_PATH,)
print("It is successfully uploaded to bucket")
