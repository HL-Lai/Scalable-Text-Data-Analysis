# Scalable Text Data Analysis

This repository contains scripts and datasets used for scalable text preprocessing and sentiment polarity calculation, leveraging the power of Apache Spark and RDDs. The data processing pipeline is divided into four sections: Data Preprocessing, Text-Punctuation Cleaning, Sentiment Polarity Calculation, and Word Count.

## Datasets

The datasets used in this project are:

1. `data_all.csv`: This is the original dataset used in the project. It is used in the first step of the data preprocessing pipeline.

2. `data_preprocessed.csv`: This is the preprocessed version of `data_all.csv`. It is generated after the punctuation cleaning step and is used in the second step of the pipeline.

## Codes

1. `data preprocess_text-punctuation cleaning.scala`: This script takes `data_all.csv` as input and performs Data Preprocessing and Text-Punctuation Cleaning on the text data. The output of this script is `data_preprocessed.csv`.

2. `textual_data_analysis.scala`: This script takes `data_preprocessed.csv` as input and performs Sentiment Polarity Calculation and Word Count operation for review text.

## Usage

To use these scripts, first run `data preprocess_text-punctuation cleaning.scala` with `data_all.csv` as input. This will generate `data_preprocessed.csv`. Then, run `textual_data_analysis.scala` with `data_preprocessed.csv` as input.

## Scalability

The scripts are designed to be scalable and efficient, capable of handling large datasets. They leverage Apache Spark and RDDs to distribute the data processing tasks across multiple nodes, if available.

## Contribution

Feel free to fork this project, open a pull request or report a bug on the issue tracker.

## License

This project is licensed under the terms of the MIT license.
