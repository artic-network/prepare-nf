#!/usr/bin/env python

import os
import sys  
import argparse
import pandas as pd 
import re
import pathlib
import glob


def load_metadata(metadata_file):
    """
    Load metadata from a CSV or XLS file.
    This function should handle both formats and return a structured format (e.g., list of dictionaries).
    """
    if metadata_file.endswith('.csv'):
        with open(metadata_file, 'r') as f:
            return pd.read_csv(f)
    elif metadata_file.endswith('.xls') or metadata_file.endswith('.xlsx'):
        with open(metadata_file, 'r') as f:
            return pd.read_excel(f)
    else:
        raise ValueError("Unsupported metadata file format. Please use CSV or XLS/XLSX.")
    
def check_metadata(metadata):
    """
    Check if the metadata contains the required columns: 'sample', 'barcode', and any additional sample information.
    """
    required_columns = ['sample', 'barcode']
    metadata_columns = metadata.columns.to_list()
    key_dict = {}
    for col in required_columns:
        for metadata_col in metadata_columns:
            if col == metadata_col.lower():
                key_dict[metadata_col] = col
                break
            elif col+"s" == metadata_col.lower():
                key_dict[metadata_col] = col
                break
            elif col+"_name" == metadata_col.lower():
                key_dict[metadata_col] = col
                break
    if len(key_dict) != len(required_columns): 
        raise ValueError(f"Metadata file is missing required columns: {', '.join([col for col in required_columns if col not in key_dict.values()])}")
    metadata.rename(columns=key_dict, inplace=True)

    print(key_dict)
        
    if not metadata['barcode'].unique().size == metadata['barcode'].size:
        raise ValueError("Metadata contains duplicate barcodes. Each barcode must be unique.")
    
    if not metadata['sample'].unique().size == metadata['sample'].size: 
        raise ValueError("Metadata contains duplicate sample names. Each sample name must be unique.")
    metadata.dropna(subset=['sample'], inplace=True)

    return True


def add_fastq_path_to_metadata(metadata, run_dir, platform):
    """
    Add the full path to the fastq files in the metadata DataFrame.
    This function assumes that the run_dir contains subdirectories named after barcodes.
    """
    if platform == 'nanopore':
        run_dir = pathlib.Path(run_dir).resolve()
        existing_paths = glob.glob(os.path.join(run_dir,"barcode*"))
        metadata['fastq_directory'] = metadata.apply(
            lambda row: os.path.join(run_dir, row['barcode']) if (os.path.join(run_dir, row['barcode']) in existing_paths) else None, axis=1
        )
        metadata.dropna(subset=['fastq_directory'], inplace=True)
    else:
        raise ValueError(f"Unsupported platform '{platform}'. Only 'ont' is currently supported.")
    
    return metadata

def add_amplicon_scheme_to_metadata(metadata, amplicon_scheme, custom_scheme_path=None):
    """
    Check if the amplicon scheme is in the correct format.
    """
    if custom_scheme_path not in [None, "", "null"]:
        # Check if the custom scheme path exists
        if not os.path.exists(custom_scheme_path):
            raise ValueError(f"Custom amplicon scheme path '{custom_scheme_path}' does not exist.")
        metadata['custom_scheme_path'] = metadata.apply(
            lambda row: custom_scheme_path, axis=1
        )
        metadata['custom_scheme_name'] = metadata.apply(
            lambda row: amplicon_scheme, axis=1
        )
    else:    
        # Example format: 'artic-inrb-mpox/2500/v1.0.0'
        # This regex checks for a scheme name, followed by a slash, a version number with at least 3 digits, another slash, and a version identifier.
        if not re.match(r'^\S*\/\d{3,}\/v\d\.\d\.\d(-\S+)?$', amplicon_scheme):
            raise ValueError("Amplicon scheme must be in the format 'scheme/version/identifier  (e.g., artic-inrb-mpox/2500/v1.0.0)'.")
        metadata['scheme_name'] = metadata.apply(
            lambda row: amplicon_scheme, axis=1
        )    
    return metadata

def add_platform_to_metadata(metadata, platform, amplicon_scheme):
    """
    Add the platform and amplicon scheme to each entry in metadata DataFrame.
    """
    metadata['platform'] = metadata.apply(
            lambda row: platform, axis=1
    )
    
    return metadata

def save_metadata(metadata, output_file='sample_sheet.csv'):
    with open(output_file, 'w') as f:
        metadata = metadata[metadata['sample'].notna()]
        metadata.to_csv(f, index=False)

def main():
    parser = argparse.ArgumentParser(description="Create sample sheet from input data.")
    parser.add_argument("--platform", type=str, help="Platform.")
    parser.add_argument("--run_dir", type=str, help="Run directory")
    parser.add_argument("--metadata", type=str, help="Metadata file (CSV or XLS).")
    parser.add_argument("--amplicon_scheme", type=str, help="Amplicon scheme identifier (e.g., artic-inrb-mpox/2500/v1.0.0")
    parser.add_argument("--custom_scheme_path", type=str, help="Absolute path to local custom amplicon scheme (if relevant)")
    args = parser.parse_args()

    args.platform = args.platform.lower()
    if args.platform == "ont":
        args.platform = "nanopore"

    # Load metadata
    metadata = load_metadata(args.metadata)
    check_metadata(metadata)

    metadata = add_fastq_path_to_metadata(metadata, args.run_dir, args.platform)

    metadata = add_amplicon_scheme_to_metadata(metadata, args.amplicon_scheme, args.custom_scheme_path)

    metadata = add_platform_to_metadata(metadata, args.platform, args.amplicon_scheme)

    save_metadata(metadata, "sample_config.csv")


if __name__ == "__main__":
    main()

# todo: only allow excel spreadsheets generated by our templates
# todo: check dates in metadata if they are specified
# todo: add support for illumina sample sheet
