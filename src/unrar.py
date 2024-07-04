"""
## --------------------------------------------------------------------------------------##
##
## Script name: unrar.py
##
## Purpose of the script: extract rar files
## Author: Chinmay Deval
##
## Created On: 06/11/2024
##
## Copyright (c) Chinmay Deval, 2024
##
##
## --------------------------------------------------------------------------------------##
"""
import rarfile
import os

def unrar_file(rar_path, extract_path):
    # Ensure the extract path exists
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)
    
    # Open the rar file
    with rarfile.RarFile(rar_path) as rf:
        # Get the base name of the RAR file (without the extension)
        base_name = os.path.splitext(os.path.basename(rar_path))[0]
        # Create a directory with the same name as the RAR file
        extract_dir = os.path.join(extract_path, base_name)
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)
        
        # Extract all contents to the specified directory
        rf.extractall(path=extract_dir)
        print(f"Extracted all files to {extract_dir}")

# Example usage
rar_path = './info Imbabura.rar' 
extract_path = './' 

unrar_file(rar_path, extract_path)