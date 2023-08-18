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

    ctl_csv_path = os.path.join(output_path, "ctl_zip_info.csv")
    txt_csv_path = os.path.join(output_path, "txt_zip_info.csv")
    write_csv(ctl_csv_path, ctl_zip_info)
    write_csv(txt_csv_path, txt_zip_info)

    ctl_mapping = get_mapping(ctl_csv_path, ".ctl.zip")
    txt_mapping = get_mapping(txt_csv_path, ".txt.zip")

    create_cross_mapping_csv(ctl_mapping, txt_mapping, output_path)

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

def get_mapping(csv_path, extension_to_remove):
    mapping = {}
    
    with open(csv_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            filename = row[0].replace(extension_to_remove, "")
            mapping[filename] = row[1]  # Store size as value

    return mapping

def create_cross_mapping_csv(ctl_mapping, txt_mapping, output_path):
    cross_mapping = []

    for txt_filename, txt_size in txt_mapping.items():
        ctl_size = ctl_mapping.get(txt_filename, "Not Found")
        cross_mapping.append([txt_filename, txt_size, ctl_size])

    cross_mapping_csv_path = os.path.join(output_path, "cross_mapping.csv")
    with open(cross_mapping_csv_path, "w", newline="") as cross_mapping_csv:
        csv_writer = csv.writer(cross_mapping_csv)
        csv_writer.writerow(["TXT Filename", "TXT Size", "CTL Size"])
        for row in cross_mapping:
            csv_writer.writerow(row)

if __name__ == "__main__":
    target_directory = "/path/to/your/unix/directory"
    output_directory = "/path/to/output/directory"
    process_directory(target_directory, output_directory)
