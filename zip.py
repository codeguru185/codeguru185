import zipfile
import os

def create_greenplum_ddl(column_names):
    ddl = "CREATE TABLE your_table_name (\n"
    for column_name in column_names:
        ddl += f"    {column_name} TEXT NOT NULL,\n"
    ddl = ddl.rstrip(",\n") + "\n);"
    return ddl

def main():
    zip_file_directory = input("Enter zip file directory: ")
    zip_file_name = input("Enter zip file name: ")
    unzip_file_directory = input("Enter unzip file directory: ")
    ddl_directory = input("Enter DDL directory: ")

    # Unzip the file
    with zipfile.ZipFile(os.path.join(zip_file_directory, zip_file_name), 'r') as zip_ref:
        zip_ref.extractall(unzip_file_directory)

    # Read the first line from the unzipped file
    unzipped_file_path = os.path.join(unzip_file_directory, zip_file_name.replace(".zip", ""))
    with open(unzipped_file_path, 'r') as file:
        first_line = file.readline()
        column_names = first_line.strip().split('|')  # Assuming pipe-delimited

    # Create DDL for Greenplum table
    ddl = create_greenplum_ddl(column_names)

    # Write DDL to a file
    base_filename = os.path.splitext(zip_file_name)[0]
    ddl_file_path = os.path.join(ddl_directory, f"{base_filename}_DDL.sql")
    with open(ddl_file_path, 'w') as ddl_file:
        ddl_file.write(ddl)

    print("Greenplum DDL generated successfully!")

if __name__ == "__main__":
    main()
