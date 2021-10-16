# Code

This folder contains the different code used to produce the index files.

- aws_file_retrieval.py scrapes the AWS directory for all available files
- building_comprehensive_aws_index.py builds the index by retrieving basic information from the available AWS files
- building_comprehensive_aws_index.ipynb does the same as its python version, but only for files readable by IRSx (2015 and later)
- updating_comprehensive_aws_index.py is the code used to update the index files. It retrieves the current list of files available and pulls the forms. Future updates will improve speed through threading and download some of the necessary input files.
