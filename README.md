# kbjohnson-penn README

To clone this repo, run the following command:

```bash
git clone https://github.com/kbjohnson-penn/philter-ucsfin
```

Philter is a command-line based clinical text de-identification software that removes protected health information (PHI) and can be used to process **plain text files**. For our use case, we have adapted Philter to be able to process **TSV** and **JSON** transcript files in the following format:

### TSV

| start | end | text  |
|-------|-----|-------|
| 190080 | 191660 | An example sentence |
| 191660 | 193240 | An example sentence |

### JSON

```json
{
    "segments": [
        {
            "start": 12.777,
            "end": 13.077,
            "text": "An example",
            "words": [
                {
                    "word": "An",
                    "start": 12.777,
                    "end": 12.937,
                    "score": 0.272,
                    "speaker": "SPEAKER_01"
                },
                {
                    "word": "example",
                    "start": 12.957,
                    "end": 13.077,
                    "score": 0.13,
                    "speaker": "SPEAKER_01"
                }
            ],
            "language": "en"
        }
    ]
}
```

For JSON input, note that Philter rewrites **both** the segment-level `text`
field and each entry in the segment's `words` list, so the word-level
timestamps stay consistent with the redacted transcript. See
[Word-level de-identification](#word-level-de-identification) for details.

# Running kbjohnson-penn Philter:

- Store all input file(s) in the same directory and make sure they are in TSV or JSON format. Examples of properly formatted input files can be found above.

- All output files will be saved in the specified output directory, which will be created if it doesn't already exist.

- De-identified JSON transcripts are written as `<stem>.philtered.json`, so
  `visit01.transcript.json` becomes `visit01.transcript.philtered.json`. The
  suffix keeps redacted output distinguishable from source transcripts at a
  glance. Files already ending in `.philtered.json` are skipped as input, so
  the same directory can safely be used for both `-i` and `-o`. (TSV output
  keeps the input filename.)

- Our current implementation uses the default configuration file. However, you can also create a configuration file with specified filters. We are currently working on identifying the best set of filters for our use case.

- Run Philter in the command line by using our custom parameters.

**-i (input):** Path to the directory that contains input (TSV and/or JSON) files<br/>
**-o (output):** Path to the directory where the output files will be written<br/>
**-f (format):** Specifies the file format (TSV or JSON) to be processed by Philter<br/>

Run Philter on the inputs file(s) by navigating to the directory containing `main_format.py` and using one of the following commands:

```bash
python3 main_format.py -i path/to/input/folder -o path/to/output/folder -f tsv
```

**or**

```bash
python3 main_format.py -i path/to/input/folder -o path/to/output/folder -f json
```

# Word-level de-identification

WhisperX JSON carries a `words` list per segment, each entry holding a token
plus its own timestamps. Downstream consumers (for example, audio redaction)
use those per-word timings to locate PHI in the audio, so the word entries must
be redacted just as thoroughly as the segment `text`.

Philter's asterisk output is a character-for-character transform: every
character of the input is either preserved, kept as punctuation, or replaced
with a single `*`. The redacted line is therefore always exactly the same
length as the input line. `main_format.py` relies on that invariant, locating
each word in the original line and slicing the same character span out of the
redacted line. This is exact, and avoids trying to pair up two token lists that
split differently on whitespace and hyphens (`Mm-hmm.`, `x-ray`) or where
Philter's own edits change the token count.

The alignment **fails closed**. If a word cannot be placed, or if the
length invariant is ever violated, the affected words are fully masked rather
than left with their original text. Every such case is recorded in
`regex_filters.log`:

```ini
20XX-XX-XX XX:XX:XX,XXX - WARNING - Could not locate word 'Example' at/after offset 0; masking it
```

If Philter returns a different number of lines than the input had segments (or
TSV rows), the run aborts with a `RuntimeError` rather than writing partially
de-identified output.

# A note on encodings

Input files are read as UTF-8. Philter detects the encoding of each intermediate
file before reading it; because statistical detection is unreliable on very
short inputs (a single transcript line can be mis-detected as a legacy codepage
and then fail to decode), valid UTF-8 is always detected as UTF-8, with
`chardet` used only as a fallback.

Undecodable bytes are preserved through the intermediate files using
`surrogateescape` and are escaped in the JSON output. A source file that is
itself not valid UTF-8 will fail loudly rather than be silently mangled --
re-encode it to UTF-8 before processing.

# kbjohnson-penn Regex Logging

Upon running Philter with `main_format.py`, a log file `regex_filters.log` will be created. This log file details the potential PHI being filtered. It includes the location of each occurrence, the original text snippet, the matched word or phrase, the regex expression used, and the starting and stopping indices of where the PHI occurred. Here is an example of how this information is presented:

```ini
20XX-XX-XX XX:XX:XX,XXX - INFO - File: 12_line_filename.txt
20XX-XX-XX XX:XX:XX,XXX - INFO - Line 12 | Text: Do you know this is an example?
20XX-XX-XX XX:XX:XX,XXX - INFO - Regex match: Do you
20XX-XX-XX XX:XX:XX,XXX - INFO - Expression: '(?i)\\bdo\\s(not|you|I)\\b'
20XX-XX-XX XX:XX:XX,XXX - INFO - Start index: 0 | End index: 6
```

**This log contains PHI in cleartext.** It records the matched snippets
themselves, not just their offsets, and it is rewritten on every run
(`filemode='w'`). Treat it with the same care as the input transcripts: it is
covered by `.gitignore`, must not be committed, and should be removed once it
is no longer needed for debugging.

`.gitignore` also excludes `*.json`, `*.csv` and `*.tsv` by default so that
transcripts and de-identified output are never committed by accident. The
repository's own configuration, filter lists and sample data are re-included
explicitly; if you add a new config or filter file outside those directories,
add a matching `!` exception rather than removing the blanket rules.

# Original Philter README

If you use this software for any publication, please cite:
Norgeot, B., Muenzen, K., Peterson, T.A. et al. Protected Health Information filter (Philter): accurately and securely de-identifying free-text clinical notes. npj Digit. Med. 3, 57 (2020). https://doi.org/10.1038/s41746-020-0258-y

# Installing Philter

To install Philter from PyPi, run the following command:

```bash
pip3 install philter-ucsf
```

The main philter code will be executed by running:

```bash
python3 -m philter_ucsf [flags, see below]
```

However, we strongly suggest that you download the project source code and run all sample commands below from the home directory before running the install version of Philter.

# Installing Requirements

To install the Python requirements, run the following command:

```bash
pip3 install -r requirements.txt
```

# Running Philter: A Step-by-Step Guide

Philter is a command-line based clinical text de-identification software that removes protected health information (PHI) from any plain text file. Although the software has built-in evaluation capabilities and can compare Philter PHI-reduced notes with a corresponding set of ground truth annotations, annotations are not required to run Philter. The following steps may be used to 1) run Philter in the command line without ground truth annotations, or 2) generate Philter-compatible annotations and run Philter in evaluation mode using ground truth annotations. Although any set of notes and corresponding annotations may be used with Philter, the examples provided here will correspond to the I2B2 dataset, which Philter uses in its default configuration. 

Before running Philter either with or without evaluation, make sure to familiarize yourself with the various options that may be used for any given Philter run:

### Flags:
**-i (input):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to the directory or the file that contains the clinical note(s), the default is ./data/i2b2_notes/<br/>
**-a (anno):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to the directory or the file that contains the PHI annotation(s), the default is ./data/i2b2_anno/<br/>
**-o (output):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to the directory to save the PHI-reduced notes in, the default is ./data/i2b2_results/<br/>
**-f (filters):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to the config file, the default is ./configs/philter_delta.json<br/>
**-x (xml):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to the json file that contains all xml data, the default is ./data/phi_notes.json<br/>
**-c (coords):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Output path to the json file that will contain the coordinate map data, the default is ./data/coordinates.json<br/>
**-v (verbose):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;When verbose is true, will emit messages about script progress. The default is True<br/>
**-e (run_eval):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;When run_eval is true, will run our eval script and emit summarized results to terminal<br/>
**-t (freq_table):**&nbsp;&nbsp;&nbsp;&nbsp;When freqtable is true, will output a unigram/bigram frequency table of all note words and their PHI/non-PHI counts. Default is False<br/>
**-n (initials):**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;When initials is true, will include annotated initials PHI in recall/precision calculations. The default is True<br/>
**--eval_output:**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to the directory that the detailed eval files will be outputted to, the default is ./data/phi/<br/>
**--outputformat:**&nbsp;&nbsp;Define format of annotation, allowed values are \"asterisk\", \"i2b2\". Default is \"asterisk\"<br/>
**--ucsfformat:**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;When ucsfformat is true, will adjust eval script for slightly different xml format. The default is False<br/>
**--prod:**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;When prod is true, this will run the script with output in i2b2 xml format without running the eval script. The default is False<br/>
**--cachepos:**&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Path to a directoy to store/load the pos data for all notes. If no path is specified then memory caching will be used<br/>

## 0. Curating I2B2 XML Files
To remove non-HIPAA PHI annotations from the I2B2 XML files, run the following command:

**-i** Path to the directory that contains the original I2B2 xml files<br/>
**-o** Path to the directory where the curated files will be written<br/>

```bash
python improve_i2b2_notes.py -i data/i2b2_xml/ -o data/i2b2_xml_updated/
```

## 1. Running Philter WITHOUT evaluation (no ground-truth annotations required)

**a.** Make sure the input file(s) are in plain text format. If you are using the I2B2 dataset (or any other dataset in XML or other formats), the note text must be extracted from each original file and be saved in individual text files. Examples of properly formatted input files can be found in ./data/i2b2_notes/.

**b.** Store all input file(s) in the same directory, and create an output directory (if you want the PHI-reduced notes to be stored somewhere other than the default location).

**c.** Create a configuration file with specified filters (if you do not want to use the default configuration file).

**d.** Run Philter in the command line using either default or custom parameters.

Use the following command to run a single job and output files in XML format:
```bash
python3 main.py -i ./data/i2b2_notes/ -o ./data/i2b2_results/ -f ./configs/philter_delta.json --prod=True
```
IMPORTANT NOTE: XML-formatted files do NOT have PHI-reduced text. Instead, they contain the original note text with the PHI tags identified by Philter. 

If you'd like to output ONLY the PHI-reduced text with asterisks obscuring Philter-identified PHI, simply add the -outputformat "asterisk" option:
```bash
python3 main.py -i ./data/i2b2_notes/ -o ./data/i2b2_results/ -f ./configs/philter_delta.json --prod=True --outputformat "asterisk"
```

To run multiple jobs simultaneously, all input notes handled by a single job must be located in separate directories to avoid cross-contamination between output files. For example, if you wanted to run Philter on 1000 notes simultaneously on two processes, the two input directories might look like:

1. ./data/batch1/500_input_notes_batch1/
2. ./data/batch2/500_input_notes_batch2/

In this example, the following two commands would be used to start running each job in the background:
```bash
nohup python3 main.py -i ./data/batch1/500_input_notes_batch2/ -o ./data/i2b2_results_test/ -f ./configs/philter_delta.json --prod=True > ./data/batch1/batch1_terminal_out.txt 2>&1 &

```
```bash
nohup python3 main.py -i ./data/batch2/500_input_notes_batch2/ -o ./data/i2b2_results_test/ -f ./configs/philter_delta.json --prod=True > ./data/batch2/batch2_terminal_out.txt 2>&1 &

```

## 2. Running Philter WITH evaluation (ground truth annotations required)

**a.** Create Philter-compatible annotation files using the transformation script located in ./generate_dataset/. This script expects notes in xml format, and transforms each input file into two plain text files: 1) the original note text, and 2) the note text with asterisks obscuring PHI. A properly formatted xml input can be found in ./data/i2b2_xml, and examples of the two outputs can be found in ./data/i2b2_notes and ./data/i2b2_anno, respectively. Additionally, this script creates a .json file that contains the original text from each note, followed by the PHI annotations in json format. An example of this output file can be found at ./data/phi_notes_i2b2.json. This is the file that will be used as the -x default option. 

### Flags:

**-x** Path to the directory file that contains the note xml files<br/>
**-o** Path to the json file that will contain a summary of the phi in the xml files<br/>
**-n** Path to the directory where you would like to store the plain text notes<br/>
**-a** Path to the directory where you would like to store the plain text annotations<br/>

Use the following command to create these input files from notes in XML format:

```bash
python3 ./generate_dataset/main_ucsf_updated.py -x ./data/i2b2_xml/ -o ./data/phi_notes_i2b2.json -n ./data/i2b2_notes/ -a ./data/i2b2_anno/
```
Note: If this command produces an ElementTree.ParseError, you may need to remove .DS_Store from ./data/i2b2_xml.

**b-c.** See Step 1b-c above

**d.** Run Philter in evaluation mode using the following command:

```bash
python3 main.py -i ./data/i2b2_notes/ -a ./data/i2b2_anno/ -o ./data/i2b2_results/ -x ./data/phi_notes_i2b2.json -f=./configs/philter_delta.json --outputformat "asterisk"
```

By defult, this will output PHI-reduced notes (.txt format) in the specified output directory. If this command is used with the --outputformat i2b2 flag (or with no --outputformat specified, since i2b2 format is the default option), the evaluation script will not be run and the script will output notes with the original text and the Philter PHI tags (.xml format) in the specified output directory.
