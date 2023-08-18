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

    # Compare the two CSV files and create additional CSV files
    create_txt_presence_csv(ctl_csv_path, txt_csv_path, output_path)
    create_ctl_presence_csv(ctl_csv_path, txt_csv_path, output_path)

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

def create_txt_presence_csv(ctl_csv_path, txt_csv_path, output_path):
    txt_presence = {}

    with open(txt_csv_path, "r") as txt_csv_file:
        csv_reader = csv.reader(txt_csv_file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            txt_filename = row[0].replace(".txt.zip", "")
            txt_presence[txt_filename] = "No"

    with open(ctl_csv_path, "r") as ctl_csv_file:
        csv_reader = csv.reader(ctl_csv_file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            ctl_filename = row[0].replace(".ctl.zip", "")
            if ctl_filename in txt_presence:
                txt_presence[ctl_filename] = "Yes"

    txt_presence_csv_path = os.path.join(output_path, "txt_presence.csv")
    with open(txt_presence_csv_path, "w", newline="") as txt_presence_csv:
        csv_writer = csv.writer(txt_presence_csv)
        csv_writer.writerow(["TXT Filename", "CTL Present"])
        for txt_filename, ctl_present in txt_presence.items():
            csv_writer.writerow([txt_filename, ctl_present])

def create_ctl_presence_csv(ctl_csv_path, txt_csv_path, output_path):
    ctl_presence = {}

    with open(ctl_csv_path, "r") as ctl_csv_file:
        csv_reader = csv.reader(ctl_csv_file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            ctl_filename = row[0].replace(".ctl.zip", "")
            ctl_presence[ctl_filename] = "No"

    with open(txt_csv_path, "r") as txt_csv_file:
        csv_reader = csv.reader(txt_csv_file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            txt_filename = row[0].replace(".txt.zip", "")
            if txt_filename in ctl_presence:
                ctl_presence[txt_filename] = "Yes"

    ctl_presence_csv_path = os.path.join(output_path, "ctl_presence.csv")
    with open(ctl_presence_csv_path, "w", newline="") as ctl_presence_csv:
        csv_writer = csv.writer(ctl_presence_csv)
        csv_writer.writerow(["CTL Filename", "TXT Present"])
        for ctl_filename, txt_present in ctl_presence.items():
            csv_writer.writerow([ctl_filename, txt_present])

if __name__ == "__main__":
    target_directory = "/path/to/your/unix/directory"
    output_directory = "/path/to/output/directory"
    process_directory(target_directory, output_directory)
