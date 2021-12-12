# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 15:53:08 2019

@author: Ismail
"""

import tarfile
from tqdm import tqdm  # pip3 install tqdm


def compress(tar_file, members):
    """
    Adds files (`members`) to a tar_file and compress it
    """
    # open file for gzip compressed writing
    tar = tarfile.open(tar_file, mode="w:gz")
    # with progress bar
    # set the progress bar
    progress = tqdm(members)
    for member in progress:
        # add file/folder/link to the tar file (compress)
        tar.add(member)
        # set the progress description of the progress bar
        progress.set_description(f"Compressing {member}")
    # close the file
    tar.close()


def decompress(tar_file, path, members, year):
    """
    Extracts `tar_file` and puts the `members` to `path`.
    If members is None, all members on `tar_file` will be extracted.
    """
    tar = tarfile.open(tar_file)
    all_members = tar.getmembers()
    all_names = [member.name for member in all_members]
    progress = tqdm(members)
    for member in progress:
        full_name = './' + member + '-99999-' + str(year) + '.op.gz'
        if full_name in all_names:
            tar.extract(full_name, path=path)
            # set the progress description of the progress bar
            progress.set_description(f"Extracting {member}")
    tar.close()