import os
import subprocess
import sys
import yaml

ignore_folders = {'__pycache__', '.ipynb_checkpoints', '.git', '.venv'}

if len(sys.argv) != 2:
    print("Root directory is required")
    exit()

root_directory = sys.argv[1]
print(f"Deploying all Snowpark apps in root directory {root_directory}")

for (directory_path, directory_names, file_names) in os.walk(root_directory):
    directory_names[:] = [name for name in directory_names if name not in ignore_folders]
    if "snowflake.yml" not in file_names:
        continue

    with open(os.path.join(directory_path, "snowflake.yml"), encoding="utf-8") as project_file:
        project = yaml.safe_load(project_file)

    if "snowpark" in project:
        print(f"Building and deploying Snowpark project in {directory_path}")
        subprocess.run(
            ["snow", "snowpark", "build", "--connection", "default"],
            cwd=directory_path,
            check=True,
        )
        subprocess.run(
            ["snow", "snowpark", "deploy", "--replace", "--connection", "default"],
            cwd=directory_path,
            check=True,
        )
    elif "streamlit" in project:
        print(f"Deploying Streamlit project in {directory_path}")
        subprocess.run(
            ["snow", "streamlit", "deploy", "--replace", "--connection", "default"],
            cwd=directory_path,
            check=True,
        )
    else:
        raise ValueError(f"Unsupported Snowflake project type in {directory_path}")
