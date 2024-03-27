from google.cloud import storage
import logging
import os


def upload_to_gcs_callable(bucket, src_files_path):
    """
    Uploads Parquet files to GCS.

    :param bucket: GCS bucket name
    :param src_files_path: List of local Parquet files to upload
    """
    # Create a GCS client
    client = storage.Client()
    bucket = client.bucket(bucket)

    # Workaround for slow connections
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    # Iterate over each Parquet file and upload it to GCS
    for local_file in src_files_path:
        print("Uploading file:", local_file)
        if not local_file.endswith(".parquet"):
            logging.error(f"Skipping non-Parquet file: {local_file}")
            continue

        # Construct the object name in GCS
        object_name = f"raw/{os.path.basename(local_file)}"

        # Upload the file to GCS
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)
