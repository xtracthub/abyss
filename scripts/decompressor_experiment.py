import argparse
import globus_sdk
import os
import pandas as pd
import shutil
import time
import zipfile
import tarfile
import gzip
from decompressor import decompress_tar_gz, decompress_zip, decompress_gz, decompress_tar, is_compressed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--dir_name", type=str, help="directory to place compressed files")
    parser.add_argument("--extract_dir", type=str, help="directory to decompress to")
    parser.add_argument("--results_csv", type=str, help="path to write results to")
    parser.add_argument("--walk_results", type=str,
                        help="path to write results of os.walk(extract_dir) to")
    parser.add_argument("--crawl_csv", type=str, help="path to .csv file containing crawl data")
    parser.add_argument("--compression_extension", type=str, help="type of file to extract (zip, gzip, tar)")
    parser.add_argument("--max_transfer_size", type=int, help="max size for globus transfers")
    parser.add_argument("--transferred_files_file", type=str, help="path to file to store paths of transferred files")
    args = parser.parse_args()

    df = pd.DataFrame(columns=["file_name", "compressed_size", "decompressed_size", "estimated_value",
                               "compression_type", "decompression_time", "estimation_time"])
    transferred_files = []
    decompressed_files = []

    # Transfer files

    petrel_endpoint = "4f99675c-ac1f-11ea-bee8-0e716405a293"
    jetstream_endpoint = "49f1efac-6049-11eb-87c8-02187389bd35"

    native_auth_client = globus_sdk.NativeAppAuthClient('7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
    native_auth_client.oauth2_start_flow()

    print("Login Here:\n\n{0}".format(native_auth_client.oauth2_get_authorize_url()))

    # Authorization code
    auth_code = str(input("Input auth code:"))

    # Create transfer client
    token_response = native_auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_access_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
    transfer_authorizer = globus_sdk.AccessTokenAuthorizer(transfer_access_token)
    transfer_client = globus_sdk.TransferClient(authorizer=transfer_authorizer)

    deep_blue_crawl_df = pd.read_csv(args.crawl_csv)
    file_uuid_mapping = dict()
    for index, row in deep_blue_crawl_df.iterrows():
        file_uuid_mapping[row[0]] = row[4]

    # Filter files
    filtered_files = deep_blue_crawl_df[deep_blue_crawl_df.file_uuid.str.endswith(args.compression_extension)].sort_values(by=["size_bytes"])

    max_size_threshold = args.max_transfer_size  # Just to make sure we don't blow up the Jetstream instance
    transferred_files = []
    batch_n = 1

    while len(filtered_files.index) > len(transferred_files) and filtered_files.iloc[[0]].size_bytes.values[0] <= max_size_threshold:
        # Pick which files to transfer
        transfer_job_size = 0
        files_to_transfer = []

        for index, row in filtered_files.iterrows():
            file_path = row[0]
            file_size = row[1]

            if file_uuid_mapping[file_path] in transferred_files:
                pass
            elif transfer_job_size + file_size > max_size_threshold:
                break
            else:
                files_to_transfer.append(file_path)
                transfer_job_size += file_size

        if not files_to_transfer:
            break

        print(f"Transferring batch {batch_n}:")
        print(f"{len(files_to_transfer)} files to transfer...")
        print(files_to_transfer)
        print(f"Total size: {transfer_job_size / (10 ** 9)} GB...")

        # Transfer data

        label = "Deep Blue transfer"
        tdata = globus_sdk.TransferData(transfer_client, petrel_endpoint,
                                        jetstream_endpoint,
                                        label=label)

        # Transfer file to file UUID to avoid name collisions
        for file in files_to_transfer:
            tdata.add_item(file, f"{os.path.join(args.dir_name, os.path.basename(file_uuid_mapping[file]))}")

        transfer_client.endpoint_autoactivate(petrel_endpoint)
        transfer_client.endpoint_autoactivate(jetstream_endpoint)

        submit_result = transfer_client.submit_transfer(tdata)
        print("Task ID:", submit_result["task_id"])

        r = transfer_client.get_task(submit_result['task_id'])

        while r.data["status"] != "SUCCEEDED":
            print("_________")
            print(f"Status: {r.data['status']}")
            print(f"Bytes transferred: {r['bytes_transferred']}, Files transferred: {r['files_transferred']}, Transfer rate: {r['effective_bytes_per_second']}")
            r = transfer_client.get_task(submit_result['task_id'])
            time.sleep(10)

        print("Transfer completed!")

        for file in files_to_transfer:
            transferred_files.append(file_uuid_mapping[file])

        try:
            num_files_to_process = len(os.listdir(args.dir_name))
            for idx, file in enumerate(os.listdir(args.dir_name)):
                try:
                    print(f"Decompressing file {idx + 1}/{num_files_to_process} ({(idx + 1)/num_files_to_process * 100}%)")
                    if not os.path.isfile(file):
                        pass

                    file_path = os.path.join(args.dir_name, file)

                    if is_compressed(file):
                        file_name = file
                        compressed_size = os.path.getsize(file_path)

                        if file_path.endswith(".zip"):
                            full_extract_dir = os.path.join(args.extract_dir,
                                                            os.path.basename(file_path)[:-4])
                            t0 = time.time()
                            decompress_zip(file_path, full_extract_dir)
                            decompression_time = time.time() - t0

                            t0 = time.time()
                            with zipfile.ZipFile(file_path, "r") as zip_f:
                                estimated_value = sum([zip_info.file_size for zip_info in zip_f.infolist()])
                            estimation_time = time.time() - t0

                            with zipfile.ZipFile(file_path, "r") as zip_f:
                                compression_types = [zip_info.compress_type for zip_info in zip_f.infolist()]
                                compression_type = max(set(compression_types), key=compression_types.count)

                            decompressed_size = 0
                            for path, subdirs, files in os.walk(full_extract_dir):
                                for decompressed_file in files:
                                    fp = os.path.join(path, decompressed_file)
                                    decompressed_files.append(fp)
                                    decompressed_size += os.path.getsize(fp)
                        elif file_path.endswith(".tar.gz"):
                            full_extract_dir = file_path[:-3]
                            print(f"EXTRACTING {file_path} to {full_extract_dir}")
                            with gzip.open(file_path, "rb") as gz_f:
                                with open(full_extract_dir, "wb") as f:
                                    f.write(gz_f.read())

                            print(f"DELETING {file_path}")
                            os.remove(file_path)

                            file_path = full_extract_dir
                            full_extract_dir = os.path.join(args.extract_dir,
                                                            os.path.basename(full_extract_dir)[:-4])
                            print(f"EXTRACTING {file_path} to {full_extract_dir}")
                            t0 = time.time()
                            with tarfile.open(file_path) as tar_f:
                                def is_within_directory(directory, target):
                                    
                                    abs_directory = os.path.abspath(directory)
                                    abs_target = os.path.abspath(target)
                                
                                    prefix = os.path.commonprefix([abs_directory, abs_target])
                                    
                                    return prefix == abs_directory
                                
                                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                                
                                    for member in tar.getmembers():
                                        member_path = os.path.join(path, member.name)
                                        if not is_within_directory(path, member_path):
                                            raise Exception("Attempted Path Traversal in Tar File")
                                
                                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                                    
                                
                                safe_extract(tar_f, full_extract_dir)
                            decompression_time = time.time() - t0

                            t0 = time.time()
                            with tarfile.open(file_path) as tar_f:
                                estimated_value = sum([tar_info.size for tar_info in tar_f.getmembers()])
                            estimation_time = time.time() - t0
                            compression_type = None

                            decompressed_size = 0
                            for path, subdirs, files in os.walk(full_extract_dir):
                                for decompressed_file in files:
                                    fp = os.path.join(path, decompressed_file)
                                    decompressed_files.append(fp)
                                    decompressed_size += os.path.getsize(fp)

                        elif file_path.endswith(".tar"):
                            full_extract_dir = os.path.join(args.extract_dir,
                                                            os.path.basename(file_path)[:-4])
                            t0 = time.time()
                            with tarfile.open(file_path) as tar_f:
                                def is_within_directory(directory, target):
                                    
                                    abs_directory = os.path.abspath(directory)
                                    abs_target = os.path.abspath(target)
                                
                                    prefix = os.path.commonprefix([abs_directory, abs_target])
                                    
                                    return prefix == abs_directory
                                
                                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                                
                                    for member in tar.getmembers():
                                        member_path = os.path.join(path, member.name)
                                        if not is_within_directory(path, member_path):
                                            raise Exception("Attempted Path Traversal in Tar File")
                                
                                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                                    
                                
                                safe_extract(tar_f, full_extract_dir)
                            decompression_time = time.time() - t0

                            t0 = time.time()
                            with tarfile.open(file_path) as tar_f:
                                estimated_value = sum([tar_info.size for tar_info in tar_f.getmembers()])
                            estimation_time = time.time() - t0
                            compression_type = None

                            decompressed_size = 0
                            for path, subdirs, files in os.walk(full_extract_dir):
                                for decompressed_file in files:
                                    fp = os.path.join(path, decompressed_file)
                                    decompressed_files.append(fp)
                                    decompressed_size += os.path.getsize(fp)

                        elif file_path.endswith(".gz"):
                            full_extract_dir = os.path.join(args.extract_dir,
                                                            os.path.basename(file_path)[:-3])
                            t0 = time.time()
                            with gzip.open(file_path, "rb") as gz_f:
                                with open(full_extract_dir, "wb") as f:
                                    f.write(gz_f.read())
                            decompression_time = time.time() - t0
                            estimation_time = None
                            compression_type = None

                            estimated_value = None

                            decompressed_size = os.path.getsize(full_extract_dir)

                        else:
                            raise ValueError(f"{file_path} is not a compressed file")

                        df.loc[len(df.index)] = [file_name, compressed_size, decompressed_size, estimated_value,
                                                 compression_type, decompression_time, estimation_time]

                        if os.path.isfile(full_extract_dir):
                            os.remove(full_extract_dir)
                        else:
                            shutil.rmtree(full_extract_dir)
                except Exception as e:
                    print(e)

            for file in os.listdir(args.dirname):
                try:
                    os.remove(os.path.join(args.dir_name, file))
                except Exception as e:
                    print(e)
            if batch_n >= 2:
                break
            batch_n += 1
        except Exception as e:
            print(f"Decompressing returned error: {e}")

    print("DONE PROCESSING ALL FILES")
    print("Writing results to disk...")
    print(len(decompressed_files))

    df.to_csv(args.results_csv, index=False)

    with open(args.transferred_files_file, "w") as f:
        for file in transferred_files:
            f.write(file + "\n")

    with open(args.walk_results, "w") as f:
        for file in decompressed_files:
            f.write(file + "\n")