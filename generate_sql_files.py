import argparse
import os.path

audit_log_directory = 'audit_log'
information_schema_directory = 'information_schema'
directories = [audit_log_directory, information_schema_directory]

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [--location <dataset-location>] <project> <output-directory>",
        description="Generate SQL files for specific projects and/or datasets into output directory."
    )
    parser.add_argument(
        'project',
        metavar='project',
        type=str,
        help='Project name'
    )
    parser.add_argument(
        '--location',
        metavar='location',
        type=str,
        help='Dataset location',
        default='region-us'
    )
    parser.add_argument(
        '--dataset',
        metavar='dataset',
        type=str,
        help='Dataset name for audit logs',
        default='doitintl-cmp-bq'
    )
    parser.add_argument(
        'output',
        metavar='output',
        type=str,
        help='Output location'
    )

    return parser

def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()

    project_name = args.project
    region = args.location
    dataset = args.dataset
    output_directory = args.output

    # If output directory doesn't exist create it
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    for current_directory in directories:
        # Cycle over each file in the directories
        for filename in os.scandir(current_directory):
            # Only grab SQL files
            if filename.is_file() and '.sql' in filename.path:
                file = open(filename.path)
                contents = file.read()
                file.close()

                # Perform a replace on the contents
                contents = contents.replace('<project-name>', project_name)
                contents = contents.replace('<dataset-region>', region)
                contents = contents.replace('<dataset>', dataset)

                # Check if output directory exists, if not create it
                output_base_path = output_directory + '/' + current_directory
                if not os.path.exists(output_directory):
                    os.mkdir(output_directory)
                if not os.path.exists(output_base_path):
                    os.mkdir(output_base_path)

                # Write the output file out
                output_filename = output_base_path +  '/' + filename.name
                output_file = open(output_filename, "w")
                output_file.write(contents)
                output_file.close()

if __name__ == "__main__":
    main()