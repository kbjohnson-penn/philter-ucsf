"""Download the NLTK corpora Philter needs.

Philter calls `nltk.pos_tag` (part-of-speech filtering) and uses
WordNetLemmatizer, both of which require data files that are NOT installed by
`pip install nltk`. Run this once per environment:

    python setup_nltk.py

Note that NLTK renamed the POS tagger resource in 3.8.2: older versions look
for `averaged_perceptron_tagger`, newer ones for
`averaged_perceptron_tagger_eng`. We request whichever the installed version
actually needs, so this works across versions.
"""

import sys

import nltk


def required_resources():
    """Resource names for the installed NLTK version."""
    resources = ["wordnet", "omw-1.4"]

    # nltk >= 3.8.2 appends a language suffix to the tagger resource.
    try:
        version = tuple(int(p) for p in nltk.__version__.split(".")[:3])
    except (AttributeError, ValueError):
        version = ()

    if version >= (3, 8, 2):
        resources.append("averaged_perceptron_tagger_eng")
        resources.append("punkt_tab")
    else:
        resources.append("averaged_perceptron_tagger")
        resources.append("punkt")

    return resources


def main():
    print(f"nltk {getattr(nltk, '__version__', 'unknown')}")

    failed = []
    for resource in required_resources():
        if nltk.download(resource, quiet=True):
            print(f"  ok      {resource}")
        else:
            print(f"  FAILED  {resource}")
            failed.append(resource)

    if failed:
        print(
            "\nCould not download: "
            + ", ".join(failed)
            + "\nCheck network access, then retry. If you are offline, copy an "
            "existing nltk_data directory onto one of the paths listed by "
            "`python -c \"import nltk; print(nltk.data.path)\"`.",
            file=sys.stderr,
        )
        return 1

    # Fail loudly here rather than partway through a de-identification run.
    nltk.pos_tag(["verification"])
    print("\nPOS tagger verified. Philter is ready to run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
