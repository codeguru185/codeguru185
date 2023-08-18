import os
import zipfile
import csv

def process_directory(directory_path, output_path):
    ctl_zip_info = []
    txt_zip_info = []

    for filename in os.listdir(directory_path):
        if filename.endswith(".ctl.zip"):
            ctl_zip_info.append(process_zip_file(directory_path, filename))
        elif filename.endswith(".txt.zip"):
            txt_zip_info.append(process_zip_file(directory_path, filename))

    write_csv(os.path.join(output_path, "ctl_zip_info.csv"), ctl_zip_info)
    write_csv(os.path.join(output_path, "txt_zip_info.csv"), txt_zip_info)

def process_zip_file(directory_path, filename):
    file_path = os.path.join(directory_path, filename)
    file_size = os.path.getsize(file_path)
    return (filename, file_size)

def write_csv(output_filename, data):
    with open(output_filename, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["Filename", "Size"])

        for row in data:
            csv_writer.writerow(row)

if __name__ == "__main__":
    target_directory = "/path/to/your/unix/directory"
    output_directory = "/path/to/output/directory"
    process_directory(target_directory, output_directory)
