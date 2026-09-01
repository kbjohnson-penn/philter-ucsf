import argparse
import logging
import pandas as pd
import json
from philter import Philter
import glob
import os
import tempfile

logging.basicConfig(
    filename='regex_filters.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)


PHILTERED_SUFFIX = ".philtered.json"


def realign_words(original, philtered, words):
    """Re-apply Philter's masking to each word-level timestamp entry.

    Philter's asterisk output is a character-for-character transform: every
    character of the input is either preserved, kept as punctuation, or
    replaced by a single "*", so `philtered` has exactly the same length as
    `original`. That lets us locate each Whisper word in the original text and
    slice the same span out of the philtered text -- exact, rather than trying
    to pair up two token lists that split differently on whitespace and
    hyphens.

    Fails closed: any word we cannot place is fully masked rather than left
    with its original (potentially PHI) text.
    """
    if len(original) != len(philtered):
        # The length invariant this relies on is broken, so no span is
        # trustworthy. Mask every word rather than emit unredacted PHI.
        logging.error(
            "Length invariant violated (original %d chars, philtered %d); "
            "masking all %d words in this segment",
            len(original), len(philtered), len(words),
        )
        for w in words:
            if w.get("word"):
                w["word"] = "*" * len(w["word"])
        return words

    cursor = 0
    for w in words:
        token = w.get("word")
        if not token:
            continue

        idx = original.find(token, cursor)
        if idx == -1:
            # Whisper occasionally reports a word that is not a literal
            # substring at/after the cursor (normalised punctuation, an
            # overlapping span). Mask it outright.
            logging.warning(
                "Could not locate word %r at/after offset %d; masking it",
                token, cursor,
            )
            w["word"] = "*" * len(token)
            continue

        w["word"] = philtered[idx:idx + len(token)]
        cursor = idx + len(token)

    return words

def process_tsv(input, output):
    input_path = os.path.join(input)
    output_path = os.path.join(output)
    files = glob.glob(os.path.join(input_path, '*.tsv'))

    for file in files:
        df = pd.read_csv(rf"{file}", sep='\t')

        f_name = os.path.splitext(os.path.basename(file))[0]
        os.makedirs(output_path, exist_ok=True)

        lines = [line for line in df["text"]]
        file_names = [f"{line_num + 1}_line_" + f"{f_name}.txt" for line_num in range(len(lines))]
        content_dict = {k: v for k, v in zip(file_names, lines)}

        with tempfile.TemporaryDirectory() as temp_dir:
            for file_name, content in content_dict.items():
                with open(os.path.join(temp_dir, file_name), 'w', encoding='utf-8') as temp_file:
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
                # errors='surrogateescape' mirrors philter.py's own write: an
                # undecodable input byte round-trips instead of raising here.
                with open(os.path.join(temp_dir, txt_file), 'r', encoding='utf-8',
                          errors='surrogateescape') as temp_file:
                    philter_lines.append(temp_file.read())

            if len(philter_lines) != len(df.index):
                raise RuntimeError(
                    f"{f_name}: Philter returned {len(philter_lines)} lines for "
                    f"{len(df.index)} rows; refusing to write partially "
                    f"de-identified output"
                )

            df["text"] = philter_lines

        df.to_csv(os.path.join(output_path, f"{f_name}.tsv"), index=False, sep='\t')

        print(f"The file {f_name}.tsv has been successfully processed and saved to the {output_path} directory.")

def process_json(input, output):
    input_path = os.path.join(input)
    output_path = os.path.join(output)
    # Skip our own output, so re-running with --input == --output does not
    # produce <stem>.philtered.philtered.json.
    files = [f for f in glob.glob(os.path.join(input_path, '*.json'))
             if not f.endswith(PHILTERED_SUFFIX)]

    for file in files:
        with open(rf"{file}", 'r', encoding='utf-8') as f:
            data = json.load(f)

        f_name = os.path.splitext(os.path.basename(file))[0]
        os.makedirs(output_path, exist_ok=True)
        segments = data["segments"]

        lines = [segments[i]["text"].strip() for i in range(len(segments))]
        file_names = [f"{line_num + 1}_line_" + f"{f_name}.txt" for line_num in range(len(lines))]
        content_dict = {k: v for k, v in zip(file_names, lines)}

        with tempfile.TemporaryDirectory() as temp_dir:
            for file_name, content in content_dict.items():
                with open(os.path.join(temp_dir, file_name), 'w', encoding='utf-8') as temp_file:
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
                # errors='surrogateescape' mirrors philter.py's own write: an
                # undecodable input byte round-trips instead of raising here.
                with open(os.path.join(temp_dir, txt_file), 'r', encoding='utf-8',
                          errors='surrogateescape') as temp_file:
                    philter_lines.append(temp_file.read())

            if len(philter_lines) != len(segments):
                raise RuntimeError(
                    f"{f_name}: Philter returned {len(philter_lines)} lines for "
                    f"{len(segments)} segments; refusing to write partially "
                    f"de-identified output"
                )

            for i in range(len(segments)):
                segments[i]["words"] = realign_words(
                    lines[i], philter_lines[i], segments[i].get("words", [])
                )
                segments[i]["text"] = philter_lines[i]

            data["segments"] = segments

        # ensure_ascii=True: philter.py reads/writes with errors='surrogateescape',
        # so an undecodable input byte can survive as a lone surrogate, which
        # cannot be encoded to strict utf-8. Escaping keeps the write atomic.
        out_name = f"{f_name}{PHILTERED_SUFFIX}"
        with open(os.path.join(output_path, out_name), 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4)

        print(f"The file {out_name} has been successfully processed and saved to the {output_path} directory.")

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