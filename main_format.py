import argparse
import logging
import pandas as pd
import json
from philter import Philter
import glob
import sys
import os
import tempfile

logging.basicConfig(
    filename='regex_filters.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

def process_tsv(input, output):
    input_path = os.path.join(input)
    output_path = os.path.join(output)
    files = glob.glob(os.path.join(input_path, '*.tsv'))

    for file in files:
        df = pd.read_csv(rf"{file}", sep='\t')

        f_name = os.path.basename(file).split('.tsv')[0]
        os.makedirs(output_path, exist_ok=True)

        lines = [line for line in df["text"]]
        file_names = [f"{line_num + 1}_line_" + f"{f_name}.txt" for line_num in range(len(lines))]
        content_dict = {k: v for k, v in zip(file_names, lines)}

        with tempfile.TemporaryDirectory() as temp_dir:
            for file_name, content in content_dict.items():
                with open(os.path.join(temp_dir, file_name), 'w') as temp_file:
                    temp_file.write(content)

            philter_config = {
                "finpath": temp_dir,
                "foutpath": temp_dir,
                "filters": "./configs/philter_delta.json",
                "verbose": False,
                "run_eval": False,
            }

            filterer = Philter(philter_config)
            filterer.map_coordinates()
            filterer.transform()

            philter_lines = []
            for txt_file in file_names:
                with open(os.path.join(temp_dir, txt_file), 'r') as temp_file:
                    philter_lines.append(temp_file.read())

            df["text"] = philter_lines

        df.to_csv(os.path.join(output_path, f"{f_name}.tsv"), index=False, sep='\t')

        print(f"The file {f_name}.tsv has been successfully processed and saved to the {output_path} directory.")

def process_json(input, output):
    input_path = os.path.join(input)
    output_path = os.path.join(output)
    files = glob.glob(os.path.join(input_path, '*.json'))

    for file in files:
        with open(rf"{file}", 'r') as f:
            data = json.load(f)

        f_name = os.path.basename(file).split('.json')[0]
        os.makedirs(output_path, exist_ok=True)
        segments = data["segments"]

        lines = [segments[i]["text"].strip() for i in range(len(segments))]
        file_names = [f"{line_num + 1}_line_" + f"{f_name}.txt" for line_num in range(len(lines))]
        content_dict = {k: v for k, v in zip(file_names, lines)}

        with tempfile.TemporaryDirectory() as temp_dir:
            for file_name, content in content_dict.items():
                with open(os.path.join(temp_dir, file_name), 'w') as temp_file:
                    temp_file.write(content)

            philter_config = {
                "finpath": temp_dir,
                "foutpath": temp_dir,
                "filters": "./configs/philter_delta.json",
                "verbose": False,
                "run_eval": False,
            }

            filterer = Philter(philter_config)
            filterer.map_coordinates()
            filterer.transform()

            philter_lines = []
            for txt_file in file_names:
                with open(os.path.join(temp_dir, txt_file), 'r') as temp_file:
                    philter_lines.append(temp_file.read())

            for i in range(len(segments)):
                segments[i]["text"] = philter_lines[i]
                for j in range(len(lines[i].split(" "))):
                    try:
                        segments[i]["words"][j]["word"] = philter_lines[i].split(" ")[j]
                    except Exception as e:
                        print(f"Error: {e}")
                        print(f"Length mismatch between {segments[i]['words']} and {philter_lines[i].split(" ")} in segment {i} of json file, start: {segments[i]['start']} end: {segments[i]['end']}")
                        sys.exit(1)

            data["segments"] = segments

        with open(os.path.join(output_path, f"{f_name}.json"), 'w') as json_file:
            json.dump(data, json_file, indent=4)
        
        print(f"The file {f_name}.json has been successfully processed and saved to the {output_path} directory.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Specify input and output directories, process TSV or JSON files.")
    parser.add_argument('-i', '--input', required=True, help="Specify the input directory.")
    parser.add_argument('-o', '--output', required=True, help="Specify the output directory.")
    parser.add_argument('-f', '--format', choices=['tsv', 'json'], required=True, help="Specify the file format to process (tsv or json).")
    args = parser.parse_args()

    if args.format == 'tsv':
        process_tsv(args.input, args.output)
    elif args.format == 'json':
        process_json(args.input, args.output)