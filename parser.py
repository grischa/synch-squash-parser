
from collections import deque
from magic import Magic
import os

from tardis.tardis_portal.models import Dataset, DataFile, DataFileObject
from tardis.tardis_portal.util import generate_file_checksums

import logging
log = logging.getLogger(__name__)


def parse_squashfs_file(exp, squash_sbox, inst,
                        directory, filename, filepath):
    dfos = DataFileObject.objects.filter(datafile__dataset__experiments=exp,
                                         uri__endswith=filepath)
    if len(dfos) == 1:
        return dfos[0]

    ignore_substrings = ['crystalpics', 'diffpics']
    for i_str in ignore_substrings:
        if filepath.find(i_str) > -1:
            return None

    basedirs = ['home', 'frames']

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
    user_subfolders = {
        'auto': {'description': 'unknown'},
    }

    dir_list = deque(directory.split(os.sep))
    first_dir = dir_list.popleft()
    if first_dir == 'frames':
        # add image missed earlier


    in_list = typical_home.get(first_dir, False)
    if in_list:
        if in_list.get('ignore', False):
            return None
        first_dir = in_list.get('dataset_name', first_dir)
    else:

    auto_ds_regex = "(xds_process)?_?([a-z0-9_-]+)_([0-9]+)_([0-9a-fA-F]+)"
    special_files = {
        'frames/.info': {'description': 'JSON'}}

    dataset = Dataset.objects.get_or_create(
        description=first_dir or 'other files',
        experiment=exp)
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
