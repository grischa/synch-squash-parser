
from collections import deque
from magic import Magic
import os
import re

from django.db.models import Q

from tardis.tardis_portal.models import Dataset, DataFile, DataFileObject
from tardis.tardis_portal.util import generate_file_checksums

import logging
log = logging.getLogger(__name__)


def parse_squashfs_file(exp, squash_sbox, inst,  # noqa # too complex
                        directory, filename, filepath):

    def get_dataset(name, directory=''):
        dataset, created = Dataset.objects.get_or_create(
            description=name, directory=directory)
        if created:
            dataset.save()
            dataset.experiments.add(exp)
        dataset.storage_boxes.add(squash_sbox)
        return dataset

    exp_q = Q(datafile__dataset__experiments=exp)
    path_part_match_q = Q(uri__endswith=filepath)
    path_exact_match_q = Q(uri=filepath)
    s_box_q = Q(storage_box=squash_sbox)
    # check whether file has been registered alread, stored elsewhere:
    dfos = DataFileObject.objects.filter(exp_q, path_part_match_q,
                                         ~s_box_q)
    if len(dfos) == 1:
        return dfos[0].datafile
    # file registered already
    dfos = DataFileObject.objects.filter(exp_q, path_exact_match_q, s_box_q)
    if len(dfos) == 1:
        return dfos[0]

    ignore_substrings = ['crystalpics', 'diffpics']
    for i_str in ignore_substrings:
        if filepath.find(i_str) > -1:
            return None

    # basedirs = ['home', 'frames']

    dir_list = deque(directory.split(os.sep))
    first_dir = dir_list.popleft()
    if first_dir != 'home':
        # add image missed earlier
        dataset = get_dataset('stray files')
    else:
        typical_home = {
            'Desktop': {'description': 'Desktop folder'},
            'Documents': {'description': 'Documents folder'},
            'Downloads': {'description': 'Downloads folder'},
            'IDLWorkspace': {'description': 'IDL Workspace folder',
                             'ignore': True},
            'Music': {'description': 'Music folder',
                      'ignore': True},
            'Pictures': {'description': 'Pictures folder'},
            'Public': {'description': 'Public folder'},
            'Templates': {'description': 'Templates folder'},
            'Videos': {'description': 'Videos folder'},
            'areavision': {'description': 'Area Vision settings',
                           'ignore': True},
            'camera_settings': {'description': 'Camera settings',
                                'ignore': True},
            'chromium': {'description': 'Chromium folder',
                         'ignore': True},
            'edm_files': {'description': 'EDM files',
                          'ignore': True},
            'google-chrome': {'description': 'bad chrome symlink',
                              'ignore': True},
            'restart_logs': {'description': 'Restart logs',
                             'ignore': True},
            'sync': {'description': 'Sync folder',
                     'ignore': True},
            'xtal_info': {'description': 'Xtal info folder (Xtalview?)',
                          'ignore': True},
            '': {'description': 'other files'},
        }
        second_dir = dir_list.popleft()
        in_list = typical_home.get(second_dir, False)
        if in_list:
            if in_list.get('ignore', False):
                return None
            dataset = get_dataset(in_list.get('dataset_name', second_dir),
                                  directory='home')
            directory = os.path.join(dir_list)
        else:
            if dir_list[1] == 'auto':
                auto_ds_regex = '(xds_process)?_?([a-z0-9_-]+)_' \
                                '([0-9]+)_([0-9a-fA-F]+)'
                match = re.findall(auto_ds_regex, filepath)
                if match:
                    match = match[0]
                    # example:
                    # ('xds_process', 'p186_p16ds1_11', '300',
                    #  '53e26bf7f6ddfc73ef2c09a8')
                    xds = match[0] == 'xds_process'
                    dataset_name = match[1]
                    number_of_images = match[2]
                    auto_id = match[3]

            dataset = get_dataset(first_dir or 'other files',
                                  directory='home')
            directory = os.path.join(*dir_list)





    filesize = inst.size(filepath)
    md5, sha512, size, mimetype_buffer = generate_file_checksums(
        inst.open(filepath))
    mimetype = ''
    if len(mimetype_buffer) > 0:
        mimetype = Magic(mime=True).from_buffer(mimetype_buffer)
    df_dict = {'dataset': dataset,
               'filename': filename,
               'directory': directory,
               'size': filesize,
               'created_time': inst.created_time(filepath),
               'modification_time': inst.modified_time(filepath),
               'mimetype': mimetype,
               'md5sum': md5,
               'sha512sum': sha512}
    df = DataFile(**df_dict)
    df.save()
    return df
