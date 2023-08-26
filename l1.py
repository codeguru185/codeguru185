import os

def generate_gpload_script(ddl_folder, ddl_file, data_folder, data_file, field_delimiter, target_db, target_schema, target_user):
    # Read DDL file to get field names
    ddl_file_path = os.path.join(ddl_folder, ddl_file)
    with open(ddl_file_path, 'r') as ddl:
        lines = ddl.readlines()
        field_names = [line.strip().split()[0] for line in lines if line.strip() and not line.startswith(');')]

    # Generate YAML content
    yaml_content = f"""\
    DATABASE: {target_db}
    USER: {target_user}
    PASSWORD: 
    HOST: 127.0.0.1
    PORT: 5432
    
    INPUT:
      - SOURCE:
          FILE:
            - {data_folder}/{data_file}
          DELIMITER: '{field_delimiter}'
        COLUMNS:
    """
    for field in field_names:
        yaml_content += f"      - {field}\n"

    yaml_content += f"""
    OUTPUT:
      - TABLE:
          SCHEMA: {target_schema}
          NAME: your_output_table_name
        MODE: INSERT
        MATCH_COLUMNS: []  # Add match columns if needed
    
    EXTERNAL:
      - SCHEMA: {target_schema}
    """

    # Write YAML content to a file
    yaml_file_path = os.path.join(data_folder, f"{data_file}_gpload.yaml")
    with open(yaml_file_path, 'w') as yaml_file:
        yaml_file.write(yaml_content)

    # Generate shell script content
    shell_script_content = f"""\
    #!/bin/bash

    export GPHOME=/path/to/greenplum
    source $GPHOME/greenplum_path.sh
    
    gpload -f {yaml_file_path}
    """

    # Write shell script content to a file
    shell_script_path = os.path.join(data_folder, f"{data_file}_gpload.sh")
    with open(shell_script_path, 'w') as shell_script:
        shell_script.write(shell_script_content)

    return shell_script_path

def main():
    ddl_folder = input("Enter DDL Folder Name: ")
    ddl_file = input("Enter DDL File Name: ")
    data_folder = input("Enter Data File Folder Name: ")
    data_file = input("Enter Data File Name: ")
    field_delimiter = input("Enter Field Delimiter: ")
    target_db = input("Enter Target DB: ")
    target_schema = input("Enter Target Schema: ")
    target_user = input("Enter Target User name: ")

    shell_script_path = generate_gpload_script(ddl_folder, ddl_file, data_folder, data_file, field_delimiter, target_db, target_schema, target_user)

    print(f"gpload shell script and YAML generated successfully!\nShell Script: {shell_script_path}")

if __name__ == "__main__":
    main()
