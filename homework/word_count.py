"""Taller evaluable"""

import glob
import os
import pandas as pd  # type: ignore


def load_input(input_directory):
    """Load text files in 'input_directory/'"""
    files = glob.glob(f"{input_directory}/*")
    dataframes = [
        pd.read_csv(file, header=None, names=["line"], sep="\n", engine="python")
        for file in files
    ]
    dataframe = pd.concat(dataframes, ignore_index=True)
    return dataframe


def clean_text(dataframe):
    """Text cleaning"""
    dataframe = dataframe.copy()
    dataframe["line"] = dataframe["line"].str.lower()
    dataframe["line"] = dataframe["line"].str.replace(r"[^a-z\s]", "", regex=True)
    return dataframe


def count_words(dataframe):
    """Word count"""
    dataframe = dataframe.copy()
    dataframe["line"] = dataframe["line"].str.split()
    dataframe = dataframe.explode("line")
    dataframe = dataframe.groupby("line").size().reset_index(name="count")
    return dataframe


def save_output(dataframe, output_directory):
    """Save output to a file."""
    if os.path.exists(output_directory):
        files = glob.glob(f"{output_directory}/*")
        for file in files:
            os.remove(file)
        os.rmdir(output_directory)

    os.makedirs(output_directory)

    dataframe.to_csv(
        f"{output_directory}/part-00000",
        sep="\t",
        index=False,
        header=False,
    )


def create_marker(output_directory):
    """Create Marker"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")


def run_job(input_directory, output_directory):
    """Job"""
    dataframe = load_input(input_directory)
    dataframe = clean_text(dataframe)
    dataframe = count_words(dataframe)
    save_output(dataframe, output_directory)
    create_marker(output_directory)


if __name__ == "__main__":
    run_job("files/input", "files/output")
