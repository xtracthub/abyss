import argparse
import os
import pandas as pd
import shutil
import zipfile
from decompressor import decompress_tar_gz, decompress_zip, decompress_gz, decompress_tar, is_compressed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--dir_name", type=str, help="directory of compressed files")
    parser.add_argument("--extract_dir", type=str, help="directory to decompress to")
    parser.add_argument("--results_csv", type=str, help="path to write results to")
    parser.add_argument("--walk_results", type=str,
                        help="path to write results of os.walk(extract_dir) to")
    args = parser.parse_args()

    df = pd.DataFrame(columns=["file_name", "compressed_size", "decompressed_size", "predicted_value"])
    decompressed_files = []

    for file in os.listdir(args.dir_name):
        if not os.path.isfile(file):
            pass

        file_path = os.path.join(args.dir_name, file)

        if is_compressed(file):
            file_name = file
            compressed_size = os.path.getsize(file_path)

            if file_path.endswith(".zip"):
                full_extract_dir = os.path.join(args.extract_dir,
                                                os.path.basename(file_path)[:-4])
                decompress_zip(file_path, full_extract_dir)

                with zipfile.ZipFile(file_path, "r") as zip_f:
                    predicted_value = sum([zip_info.file_size for zip_info in zip_f.infolist()])

            elif file_path.endswith(".tar.gz"):
                full_extract_dir = os.path.join(args.extract_dir,
                                                os.path.basename(file_path)[:-7])
                decompress_tar_gz(file_path, full_extract_dir)

                predicted_value = None

            elif file_path.endswith(".tar"):
                full_extract_dir = os.path.join(args.extract_dir,
                                                os.path.basename(file_path)[:-4])
                decompress_tar(file_path, full_extract_dir)

                predicted_value = None

            elif file_path.endswith(".gz"):
                full_extract_dir = os.path.join(args.extract_dir,
                                                os.path.basename(file_path)[:-3])
                decompress_gz(file_path, full_extract_dir)

                predicted_value = None

            else:
                raise ValueError(f"{file_path} is not a compressed file")

            decompressed_size = 0
            for path, subdirs, files in os.walk(full_extract_dir):
                for decompressed_file in files:
                    fp = os.path.join(path, decompressed_file)
                    decompressed_files.append(fp)
                    decompressed_size += os.path.getsize(fp)

            df.loc[len(df.index)] = [file_name, compressed_size, decompressed_size, predicted_value]

            if os.path.isfile(full_extract_dir):
                os.remove(full_extract_dir)
            else:
                shutil.rmtree(full_extract_dir)

    df.to_csv(args.results_csv)

    with open(args.walk_results, "w") as f:
        for file in decompressed_files:
            f.write(file + "\n")




