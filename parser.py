import ast
import os
import re

from collections import deque
from magic import Magic

from django.db.models import Q

from tardis.tardis_portal.models import Dataset, DataFile, DataFileObject
from tardis.tardis_portal.models import ParameterName, \
    Schema, DatasetParameterSet, DatasetParameter
from tardis.tardis_portal.util import generate_file_checksums

import logging
log = logging.getLogger(__name__)


def parse_squashfs_box_data(exp, squash_sbox, inst):
    '''
    squash file metadata

    path: frames/.info
    example contents:
        {'EPN': '8020l',
         u'PI': {u'Email': u'tom.caradoc-davies@synchrotron.org.au',
                 u'Name': u'Tom Caradoc-Davies',
                 u'ScientistID': u'783'},
         u'finishBooking': u'2014-07-12 08:00:00',
         u'handover': None,
         u'proposalType': u'MD',
         u'startBooking': u'2014-07-11 08:00:00',
         u'users': []}

        {'EPN': '8107b',
         u'PI': {u'Email': u'maria.hrmova@adelaide.edu.au',
                 u'Name': u'Maria Hrmova',
                 u'ScientistID': u'1886'},
         u'finishBooking': u'2014-08-01 16:00:00',
         u'handover': None,
         u'proposalType': u'CBR',
         u'startBooking': u'2014-08-01 08:00:00',
         u'users': [{u'Email': u'maria.hrmova@adelaide.edu.au',
                     u'Name': u'Maria Hrmova',
                     u'ScientistID': u'1886'},
                    {u'Email': u'victor.streltsov@csiro.au',
                     u'Name': u'Victor Streltsov',
                     u'ScientistID': u'183'}]}

    '''
    info_path = 'frames/.info'
    with inst.open(info_path) as info_file:
        info = ast.literal_eval(info_file.read())

    def transform_name(name):
        f_name, l_name = name.split(' ')
        u_name = l_name.replace('-', '') + f_name[0]
        u_name = u_name.lower()
        return u_name

    try:
        info['usernames'] = {
            transform_name(info['PI']['Name']): info['PI']}
        for user in info['users']:
            info['usernames'][transform_name(user['Name'])] = user
    except:
        pass

    return info


def parse_squashfs_file(exp, squash_sbox, inst,  # noqa # too complex
                        directory, filename, filepath, box_data=None):

    def get_dataset(name, directory=''):
        dataset, created = Dataset.objects.get_or_create(
            description=name, directory=directory)
        if created:
            dataset.save()
            dataset.experiments.add(exp)
        dataset.storage_boxes.add(squash_sbox)
        return dataset

    def tag_with_user_info(dataset, username):
        if username not in box_data['usernames']:
            return
        ns = 'http://synchrotron.org.au/userinfo'
        schema, created = Schema.objects.get_or_create(
            name="Synchrotron User Information",
            namespace=ns,
            type=Schema.NONE,
            hidden=True)
        ps, created = DatasetParameterSet.objects.get_or_create(
            schema=schema, dataset=dataset)
        pn_name, created = ParameterName.objects.get_or_create(
            schema=schema,
            name='name',
            full_name='Full Name',
            data_type=ParameterName.STRING
        )
        pn_email, created = ParameterName.objects.get_or_create(
            schema=schema,
            name='email',
            full_name='email address',
            data_type=ParameterName.STRING
        )
        pn_scientistid, created = ParameterName.objects.get_or_create(
            schema=schema,
            name='scientistid',
            full_name='ScientistID',
            data_type=ParameterName.STRING
        )
        data = box_data['usernames'][username]
        p_name, created = DatasetParameter.objects.get_or_create(
            name=pn_name, parameterset=ps)
        if p_name.string_value is None or p_name.string_value == '':
            p_name.string_value = data['Name']
            p_name.save()
        p_email, created = DatasetParameter.objects.get_or_create(
            name=pn_email, parameterset=ps)
        if p_email.string_value is None or p_name.string_value == '':
            p_email.string_value = data['Email']
            p_email.save()
        p_scientistid, created = DatasetParameter.objects.get_or_create(
            name=pn_scientistid, parameterset=ps)
        if p_scientistid.string_value is None or \
           p_scientistid.string_value == '':
            p_scientistid.string_value = data['ScientistID']
            p_scientistid.save()

    def store_auto_id(dataset, auto_id):
        ns = 'http://synchrotron.org.au/autoprocessing/xds'
        schema, created = Schema.objects.get_or_create(
            name="Synchrotron Auto Processing Results",
            namespace=ns,
            type=Schema.NONE,
            hidden=True)
        ps, created = DatasetParameterSet.objects.get_or_create(
            schema=schema, dataset=dataset)
        pn_mongoid, created = ParameterName.objects.get_or_create(
            schema=schema,
            name='mongo_id',
            full_name='Mongo DB ID',
            data_type=ParameterName.STRING
        )
        p_mongoid, created = DatasetParameter.objects.get_or_create(
            name=pn_mongoid, parameterset=ps)
        if p_mongoid.string_value is None or p_mongoid.string_value == '':
            p_mongoid.string_value = auto_id
            p_mongoid.save()

    ignore_substrings = ['crystalpics', 'diffpics']
    for i_str in ignore_substrings:
        if filepath.find(i_str) > -1:
            return None

    def remove_dotslash(path):
        if path[0:2] == './':
            return path[2:]
        return path

    directory = remove_dotslash(directory)
    filepath = remove_dotslash(filepath)
    dir_list = deque(directory.split(os.sep))

    exp_q = Q(datafile__dataset__experiments=exp)
    path_part_match_q = Q(uri__endswith=filepath)
    path_exact_match_q = Q(uri=filepath)
    s_box_q = Q(storage_box=squash_sbox)
    # check whether file has been registered already, stored elsewhere:
    dfos = DataFileObject.objects.filter(exp_q, path_part_match_q,
                                         ~s_box_q).select_related(
                                             'datafile', 'datafile__dataset')
    if len(dfos) == 1:
        df = dfos[0].datafile
        if df.dataset.directory is None or df.dataset.directory == '':
            df.dataset.directory = directory
            df.dataset.save()
        df.add_original_path_tag(directory, replace=False)
        if dir_list[0] == 'frames':
            tag_with_user_info(df.dataset, dir_list[1])
        return df
    # file registered already
    dfos = DataFileObject.objects.filter(exp_q, path_exact_match_q, s_box_q)
    if len(dfos) == 1:
        return dfos[0]

    # basedirs = ['home', 'frames']

    try:
        first_dir = dir_list.popleft()
    except IndexError:
        dataset = get_dataset('other files', directory='')
    if first_dir != 'home':
        # add image missed earlier
        dataset = get_dataset('stray files')
    else:
        dataset = None
        # first_dir == home
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
        try:
            second_dir = dir_list.popleft()
        except IndexError:
            second_dir = ''
        in_list = typical_home.get(second_dir, False)
        if in_list:
            if in_list.get('ignore', False):
                return None
            dataset = get_dataset(in_list.get('dataset_name', second_dir),
                                  directory='home')
            directory = os.path.join(dir_list)
        else:
            # second_dir == username most likely
            # dir_list == user files
            if len(dir_list) > 0 and dir_list[0] == 'auto':
                # store username somewhere for future
                dataset_name = None
                if len(dir_list) > 1 and dir_list[1] == 'index':
                    auto_index_regex = '([A-Za-z0-9_]+)_([0-9]+)_' \
                                       '([0-9]+)(failed)?$'
                    match = re.findall(auto_index_regex, filepath)
                    if match:
                        match = match[0]
                        dataset_name = match[0]

                        # match index01.out file for
                        #  image FILENAME:
# /data/8020l/frames/calibration/test_crystal/testcrystal_0_001.img
                        filename_regex = ' image FILENAME: (.+)'
                        index01_filename = os.path.join(
                            first_dir, second_dir,
                            dir_list[0], dir_list[1], dir_list[2],
                            'index01.out')
                        with inst.open(index01_filename, 'r') as indexfile:
                            index01_contents = indexfile.read()
                        image_filename_match = re.findall(filename_regex,
                                                          index01_contents)
                        if len(image_filename_match) == 1:
                            image_filename = image_filename_match[0]
                            image_filename = '/'.join(
                                image_filename.split('/')[2:])
                        img_dfos = DataFileObject.objects.filter(
                            uri__endswith=image_filename)
                        if len(img_dfos) > 0:
                            dataset = img_dfos[0].datafile.dataset
                        # number_of_images = match[1]
                elif len(dir_list) > 1 and dir_list[1] == 'dataset':
                    auto_ds_regex = '(xds_process)?_?([a-z0-9_-]+)_' \
                                    '([0-9]+)_([0-9a-fA-F]+)'
                    match = re.findall(auto_ds_regex, filepath)
                    if match:
                        match = match[0]
                        # example:
                        # ('xds_process', 'p186_p16ds1_11', '300',
                        #  '53e26bf7f6ddfc73ef2c09a8')
                        xds = match[0] == 'xds_process'
                        # store mongo id for xds ones
                        dataset_name = match[1]
                        # symlink matching
                        # number_of_images = match[2]
                        auto_id = match[3]
                        if len(dir_list) > 2:
                            # actual results, not summary files
                            link_filename = inst.path(os.path.join(
                                first_dir, second_dir,
                                dir_list[0], dir_list[1], dir_list[2],
                                'img'))
                            dataset_path = os.readlink(link_filename)
                            dataset_path = '/'.join(
                                dataset_path.split('/')[2:])
                            img_dfos = DataFileObject.objects.filter(
                                uri__startswith=dataset_path)
                            if len(img_dfos) > 0:
                                dataset = img_dfos[0].datafile.dataset
                            if xds:
                                store_auto_id(dataset, auto_id)
                elif len(dir_list) > 1 and dir_list[1] == 'rickshaw':
                    dataset = get_dataset('rickshaw_auto_processing')
                    # store like 'dataset' auto processing
                if dataset is None and dataset_name is None:
                    dataset_name = 'other auto processing'
                else:
                    dataset_name += ' auto_id: %s' % auto_id
                    dataset = dataset or get_dataset(
                        dataset_name, directory='home/auto_processing')
            else:
                dataset = get_dataset(first_dir or 'other files',
                                      directory='home')
            directory = os.path.join(second_dir, *dir_list)
            tag_with_user_info(dataset, second_dir)

    # to complete function need to set these vars above:
    # - filepath
    # - dataset
    # - filename
    # - directory
    md5, sha512, size, mimetype_buffer = generate_file_checksums(
        inst.open(filepath))
    mimetype = ''
    if len(mimetype_buffer) > 0:
        mimetype = Magic(mime=True).from_buffer(mimetype_buffer)
    df_dict = {'dataset': dataset or get_dataset('other files', directory=''),
               'filename': filename,
               'directory': directory,
               'size': size,
               'created_time': inst.created_time(filepath),
               'modification_time': inst.modified_time(filepath),
               'mimetype': mimetype,
               'md5sum': md5,
               'sha512sum': sha512}
    try:
        df = DataFile.objects.get(directory=directory,
                                  filename=filename,
                                  dataset=dataset)
        for key, value in df_dict.items():
            setattr(df, key, value)
    except DataFile.DoesNotExist:
        df = DataFile(**df_dict)
    df.save()
    df.add_original_path_tag(directory, replace=True)
    return df
