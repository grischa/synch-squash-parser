import ast
import os
import re

from magic import Magic

from django.contrib.contenttypes.models import ContentType
from django.db.models import Q

from tardis.tardis_portal.models import (
    Dataset, DataFile, DataFileObject,
    ParameterName, DatafileParameterSet,
    ExperimentParameter,
    Schema, DatasetParameterSet, DatasetParameter,
    StorageBox, StorageBoxOption
)
from tardis.tardis_portal.util import generate_file_checksums

import logging
log = logging.getLogger(__name__)

IGNORE_PATH_SUBSTRINGS = ['crystalpics', 'diffpics']

TYPICAL_HOME = {
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


def get_or_create_storage_box(datafile):
    key_name = 'datafile_id'
    class_name = 'tardis.tardis_portal.storage.squashfs.SquashFSStorage'
    try:
        s_box = StorageBoxOption.objects.get(
            key=key_name, value=datafile.id,
            storage_box__django_storage_class=class_name).storage_box
    except StorageBoxOption.DoesNotExist:
        s_box = StorageBox(
            django_storage_class=class_name,
            max_size=datafile.size,
            status='empty',
            name=datafile.filename,
            description='SquashFS Archive in DataFile id: %d, filename: %s' %
            (datafile.id, datafile.filename)
        )
        s_box.save()
        StorageBoxOption(key=key_name, value=datafile.id,
                         storage_box=s_box).save()
    return s_box


def get_squashfs_metadata(squash_sbox):
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
    inst = squash_sbox.get_initialised_storage_instance()
    with inst.open(info_path) as info_file:
        info = ast.literal_eval(info_file.read())

    def transform_name(name):
        '''
        create short name from last name and first character of first name
        '''
        f_name, l_name = name.split(' ')
        u_name = l_name + f_name[0]
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


def file_registered(experiment, s_box, filepath):
    try:
        DataFileObject.objects.get(
            storage_box=s_box,
            uri=filepath,
            datafile__dataset__experiments=experiment)
    except:
        return False
    return True


def get_file_details(experiment, s_box, filepath, filename):
    inst = s_box.get_initialised_storage_instance()
    try:
        md5, sha512, size, mimetype_buffer = generate_file_checksums(
            inst.open(filepath))
    except IOError as e:
        log.debug('squash parse error')
        log.debug(e)
        if os.path.islink(inst.path(filepath)):
            return None
        raise
        # return None
    try:
        existing_df = DataFile.objects.get(
            filename=filename,
            md5sum=md5,
            size=size,
            dataset__experiments=experiment)
    except DataFile.DoesNotExist:
        existing_df = None
    return {
        'existing_df': existing_df,
        'md5': md5,
        'sha512': sha512,
        'size': size,
        'mimetype_buffer': mimetype_buffer,
        'created_time': inst.created_time(filepath),
        'modification_time': inst.modified_time(filepath),
    }


def tag_with_user_info(dataset, metadata, username):
    if username not in metadata['usernames']:
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
    data = metadata['usernames'][username]
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


def create_datafile(dataset, basedir, filename, s_box, file_details):
    try:
        df = DataFile.objects.get(
            directory=basedir, filename=filename, dataset=dataset)
        return df
    except DataFile.DoesNotExist:
        pass
    mimetype = ''
    if len(file_details['mimetype_buffer']) > 0:
        mimetype = Magic(mime=True).from_buffer(
            file_details['mimetype_buffer'])

    df_dict = {'dataset': dataset,
               'filename': filename,
               'directory': basedir,
               'size': str(file_details['size']),
               'created_time': file_details['created_time'],
               'modification_time': file_details['modified_time'],
               'mimetype': mimetype,
               'md5sum': file_details['md5'],
               'sha512sum': file_details['sha512']}
    df = DataFile(**df_dict)
    df.save()
    return df


def parse_frames_file(basedir, filename, s_box):
    dataset_ids = list(DataFileObject.objects.filter(uri__contains=basedir)
                       .values_list('datafile__dataset__id', flat=True))
    if len(dataset_ids) == 0:
        ds = Dataset(description=basedir.split(os.sep)[-1],
                     directory=basedir)
        ds.save()
    else:
        dataset_id = max(set(dataset_ids), key=dataset_ids.count)
        ds = Dataset.objects.get(id=dataset_id)

    if ds.directory is None or ds.directory == '':
        ds.directory = basedir
        ds.save()
    return ds


def store_auto_id(dataset, auto_id):
    ns = 'http://synchrotron.org.au/mx/autoprocessing/xds'
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


def get_or_create_dataset(description, directory, experiment):
    existing = Dataset.objects.filter(description=description,
                                      directory=directory,
                                      experiments=experiment)
    if len(existing) > 0:
        return existing[0]
    ds = Dataset(description=description,
                 directory=directory)
    ds.save()
    ds.experiments.add(experiment)
    return ds


def auto_processing_link(raw_dataset, auto_dataset):
    auto_processing_schema = 'http://store.synchrotron.org.au/mx/auto_link'
    schema, created = Schema.objects.get_or_create(
        name="AU Synchrotron MX auto processing link",
        namespace=auto_processing_schema,
        type=Schema.DATASET,
        hidden=False)
    ps, created = DatasetParameterSet.objects.get_or_create(
        schema=schema, dataset=raw_dataset)
    pn, created = ParameterName.objects.get_or_create(
        schema=schema,
        name="auto processing results",
        full_name="Link to dataset containing auto processing results",
        data_type=ParameterName.LINK
    )
    par, created = DatasetParameter.objects.get_or_create(
        name=pn,
        parameterset=ps,
        link_id=auto_dataset.id,
        link_ct=ContentType.objects.get_for_model(Dataset)
    )


def parse_auto_processing(basedir, filename, s_box,
                          path_elements, experiment):
    filepath = os.path.join(basedir, filename)
    inst = s_box.get_initialised_storage_instance()
    dataset_name = None
    raw_dataset = None
    if len(path_elements) > 3 and path_elements[3] == 'index':
        auto_index_regex = '([A-Za-z0-9_]+)_([0-9]+)_' \
                           '([0-9]+)(failed)?$'
        match = re.findall(auto_index_regex, filepath)
        if match:
            match = match[0]
            if match[3] == 'failed':
                return None
            dataset_name = match[0] + 'auto processing'
            directory = os.path.join(*path_elements[:3])
            # match index01.out file for
            #  image FILENAME:
            # /data/8020l/frames/calibration/test_crystal/testcrystal_0_001.img
            filename_regex = ' image FILENAME: (.+)'
            index01_filename = os.path.join(
                path_elements[0],  # 'home'
                path_elements[1],  # username
                path_elements[2],  # 'auto'
                path_elements[3],  # 'index'
                path_elements[4],  # name
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
                datafile__dataset__experiments=experiment,
                uri__endswith=image_filename)
            if len(img_dfos) > 0:
                raw_dataset = img_dfos[0].datafile.dataset
            # number_of_images = match[1]
        else:
            dataset_name = 'auto processing - unmatched'
            directory = os.path.join(
                *os.path.join(path_elements[:min(3, len(path_elements))]))
    elif len(path_elements) > 4 and path_elements[3] == 'dataset':
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
            dataset_name = match[1] + 'auto processing'
            directory = os.path.join(*path_elements[:5])
            # symlink matching
            # number_of_images = match[2]
            auto_id = match[3]
            if len(path_elements) > 4:
                # actual results, not summary files
                link_filename = inst.path(os.path.join(
                    path_elements[0],
                    path_elements[1],
                    path_elements[2],
                    path_elements[3],
                    path_elements[4],
                    'img'))
                dataset_path = os.readlink(link_filename)
                dataset_path = '/'.join(
                    dataset_path.split('/')[2:])
                img_dfos = DataFileObject.objects.filter(
                    datafile__dataset__experiments=experiment,
                    uri__startswith=dataset_path)
                if len(img_dfos) > 0:
                    raw_dataset = img_dfos[0].datafile.dataset
                if xds:
                    store_auto_id(raw_dataset, auto_id)
        else:
            dataset_name = 'auto processing - unmatched'
            directory = os.path.join(
                *os.path.join(path_elements[:min(5, len(path_elements))]))
    elif len(path_elements) > 4 and path_elements[3] == 'rickshaw':
        dataset_name = 'auto rickshaw'
        directory = os.path.join(*path_elements[:3])
    else:
        dataset_name = 'auto process - unmatched'
        dir_length = min(len(path_elements), 4)
        directory = os.path.join(*path_elements[:dir_length])
    ds = get_or_create_dataset(
        description=dataset_name,
        directory=directory,
        experiment=experiment)
    if raw_dataset is not None:
        auto_processing_link(raw_dataset, ds)
    return ds


def parse_home_dir_file(basedir, filename, s_box, path_elements,  # noqa
                        metadata, experiment):
    path_length = len(path_elements)

    if path_length < 2:
        first_dir = ''
    else:
        first_dir = path_elements[1]
    # typical, see top of file for settings
    typical = TYPICAL_HOME.get(first_dir, None)
    if typical is not None:
        if typical.get('ignore', False):
            return None
        ds_description = typical['description']
        if path_length > 1:
            directory = os.path.join(*path_elements[:1])
        else:
            directory = ''
        ds = get_or_create_dataset(description=ds_description,
                                   directory=directory,
                                   experiment=experiment)
    else:
        # everything else
        if path_length > 2 and path_elements[2] == 'auto':
            ds = parse_auto_processing(
                basedir, filename, s_box, path_elements, experiment)
        else:
            desc_length = 2 if path_length > 2 else 1
            ds = get_or_create_dataset(
                description=os.path.join(*path_elements[:desc_length]),
                directory=os.path.join(*path_elements[:desc_length]),
                experiment=experiment)
    if path_length > 1 and ds is not None:
        tag_with_user_info(ds, metadata, path_elements[1])
    return ds


def parse_file(experiment, s_box, basedir, filename, metadata):
    filepath = os.path.join(basedir, filename)

    # ignore some files
    for i_str in IGNORE_PATH_SUBSTRINGS:
        if filepath.find(i_str) > -1:
            return None, None

    if file_registered(experiment, s_box, filepath):
        return None, None

    file_details = get_file_details(experiment, s_box, filepath, filename)
    if file_details is None:
        return None, None
    if file_details['existing_df'] is not None:
        df = file_details['existing_df']
        ds = df.dataset
        return df, ds

    path_elements = basedir.split(os.sep)
    ds = None
    if len(path_elements) > 0:
        first_dir = path_elements[0]
        if first_dir == 'frames':
            ds = parse_frames_file(
                basedir, filename, s_box)
            tag_with_user_info(ds, metadata, path_elements[1])
        elif first_dir == 'home':
            ds = parse_home_dir_file(basedir, filename, s_box,
                                     path_elements, metadata, experiment)
    if ds is None:
        if len(path_elements) > 0:
            directory = os.path.join(
                *path_elements[:min(len(path_elements), 2)])
        else:
            directory = ''
        ds = get_or_create_dataset(
            description='other files',
            directory=directory,
            experiment=experiment)
    df = create_datafile(ds, basedir, filename, s_box, file_details)
    df.add_original_path_tag(basedir, replace=False)
    if experiment not in ds.experiments.all():
        ds.experiments.add(experiment)
    return df, ds


def remove_dotslash(path):
    if path[0:2] == './':
        return path[2:]
    return path


def parse_squashfs_file(squashfile, ns):
    epn = DatafileParameterSet.objects.get(
        datafile=squashfile,
        schema__namespace=ns
    ).datafileparameter_set.get(
        name__name='EPN'
    ).string_value

    exp_ns = 'http://www.tardis.edu.au/schemas/as/experiment/2010/09/21'
    parameter = ExperimentParameter.objects.get(
        name__name='EPN',
        name__schema__namespace=exp_ns,
        string_value=epn)
    experiment = parameter.parameterset.experiment
    s_box = get_or_create_storage_box(squashfile)
    metadata = get_squashfs_metadata(s_box)

    sq_inst = s_box.get_initialised_storage_instance()
    for basedir, dirs, files in sq_inst.walk():
        for filename in files:
            clean_basedir = remove_dotslash(basedir)
            df, ds = parse_file(experiment, s_box,
                                clean_basedir, filename,
                                metadata)
            if df is None or ds is None:
                continue
            uri = os.path.join(clean_basedir, filename)
            dfos = DataFileObject.objects.filter(
                datafile=df,
                uri=uri,
                storage_box=s_box)
            if len(dfos) == 0:
                dfo = DataFileObject(
                    datafile=df,
                    uri=uri,
                    storage_box=s_box)
                dfo.save()
