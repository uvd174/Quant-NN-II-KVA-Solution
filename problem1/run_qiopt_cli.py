import time
from pathlib import Path
import argparse
from qctl.core.cloud_platform_client import CloudPlatformClient
from itertools import islice


CLOUD_PLATFORM_URL = "https://cloudos.qboard.tech"
WORKSPACE_ID = "workspace-59693eed57c441bc9aa5af4a8b931be8"  # workspace should be already exists
IMAGE_NAME = "cloud-platform-quantum-modules-qboard"  # use correct image name


def print_status(msg: str):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} {msg}")


def process_file(input_file_path: Path, output_file_path: Path, use_gpu: bool, user_data_path: Path):
    """Processes the input file and writes the results to the output file."""

    if not input_file_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file_path}")

    if output_file_path and not output_file_path.parent.exists():  # Check if parent directory exists
        raise FileNotFoundError(f"Output directory does not exist: {output_file_path.parent}")

    if not user_data_path.exists():  # Check if parent directory exists
        raise FileNotFoundError(f"User data file does not exist: {output_file_path.parent}")

    try:
        with open(user_data_path, mode="r", encoding="utf-8") as f:
            username, password = [line.strip() for line in islice(f, 2)]
            print_status(f"{username=}")
    except OSError:
        print_status(f"ERROR: Could not open/read file: {user_data_path.name}")

    run_cloudos_task(input_file_path, output_file_path, use_gpu, username, password)


def run_cloudos_task(input_file_path: Path, output_file_path: Path, use_gpu: bool, username: str, password: str):
    # auth process
    client = CloudPlatformClient(cloud_platform_url=CLOUD_PLATFORM_URL)

    client.login(username=username, password=password)
    input_filename = input_file_path.name
    if output_file_path:
        output_filename = output_file_path.name
    else:
        output_filename = Path("output.json")
    # putting file on the server
    print_status(f"Loading file {input_file_path} on the server...")
    client.put_file(source_path=input_file_path, dest_path=input_filename, workspace_id=WORKSPACE_ID)
    print_status(f"File {input_filename} loaded on the server.")

    # creating qiopt process
    print_status("Creating process...")

    default_args = [
            "solve",
            "--matrix",
            f"/workspace/{input_filename}",
            "--num-runs",
            "10",
            "--num-steps",
            "1000",
            "--output",
            f"/workspace/{output_filename}",
            "--verbose",
            "1",
        ]

    if use_gpu:
        default_args.append("--gpu")

    process = client.create_process(
        workspace_id=WORKSPACE_ID,
        image=IMAGE_NAME,
        name="run qiopt",
        cpu="4",
        ram="4",
        gpu=int(use_gpu == True),
        command="qiopt",
        args=default_args,
    )
    print_status("Process created.")

    # Waiting for process to complete
    print_status("Waiting for process to complete...")
    while process["status"] != "COMPLETED":
        if process["status"] in ["CREATED", "SUBMITTED", "RUNNING", "COMPLETED"]:
            time.sleep(1)
        else:
            msg = client.get_process_output(process["id"])
            print_status("The process was not completed normally")
            raise ValueError(msg)
        process = client.describe_process(process["id"])
    print_status("Process completed.")

    # print process output
    process_output = client.get_process_output(process["id"])
    print_status(f"Process_output: {process_output}")

    # load file from server to local dir
    if output_file_path:
        print_status(f"Getting output file {output_filename} from the server...")
        client.get_file(source_path=output_filename, dest_path=output_file_path, workspace_id=WORKSPACE_ID)
        print_status(f"File saved locally to {output_file_path}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a file and write the results to another file.", allow_abbrev=False)
    parser.add_argument("--input-file", "-i", type=Path, required=True, help="Path to the input file. Should be in edgelist format!")
    parser.add_argument("--output-file", "-o", type=Path, required=False, help="Path to the output file, json format")
    parser.add_argument("--user-data-file", "-u", type=Path, required=False, help="Path to the user data file.", default="./USER_DATA.txt")
    parser.add_argument("--gpu", action='store_true', help="Use GPU.")
    parser.add_argument("--run-id", type=int, required=False, help="Run id for parallel launch.", default=-1)

    args = parser.parse_args()

    if args.run_id != -1:
        output_file_path = args.output_file.with_name(f"{args.run_id}_{args.output_file.name}") 
    else:
        output_file_path = args.output_file

    try:
        process_file(args.input_file, output_file_path, args.gpu, args.user_data_file)
    except FileNotFoundError as e:
        print_status(f"Error: {e}")
        exit(1)  # Exit with error code
    except Exception as e:
        print_status(f"An unexpected error occurred: {e}")
        exit(1)  # Exit with error code
