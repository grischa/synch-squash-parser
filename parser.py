import ast
import os
import re

from magic import Magic

from django.contrib.contenttypes.models import ContentType

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


def tag_with_user_info(dataset, metadata, username):
    if 'usernames' not in metadata:
        return
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
            dataset_name = match[0] + ' auto processing'
            directory = os.path.join(
                *path_elements[:min(5, len(path_elements))])
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
            naming_dir_i = min(5, len(path_elements))
            dataset_name = 'auto processing - %s' % path_elements[naming_dir_i]
            directory = os.path.join(
                *os.path.join(path_elements[:naming_dir_i]))
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
            dataset_name = match[1] + ' auto processing'
            directory = os.path.join(
                *path_elements[:min(5, len(path_elements))])
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
            naming_dir_i = min(5, len(path_elements))
            dataset_name = 'auto processing - %s' % path_elements[naming_dir_i]
            directory = os.path.join(
                *os.path.join(path_elements[:naming_dir_i]))
    elif len(path_elements) > 4 and path_elements[3] == 'rickshaw':
        dataset_name = 'auto rickshaw'
        directory = os.path.join(*path_elements[:4])
    else:
        naming_dir_i = min(5, len(path_elements))
        dataset_name = 'auto process - %s' % path_elements[naming_dir_i]
        directory = os.path.join(*path_elements[:naming_dir_i])
    ds = get_or_create_dataset(
        description=dataset_name,
        directory=directory,
        experiment=experiment)
    if raw_dataset is not None:
        auto_processing_link(raw_dataset, ds)
    return ds


def remove_dotslash(path):
    if path[0:2] == './':
        return path[2:]
    return path


def update_dataset(dataset, top):
    if not dataset.directory.startswith(top):
        dataset.directory = top
        dataset.save()


class ASSquashParser(object):
    '''
    if frames:
        files: .info
        directories:
            if calibration:
                all into calibration dataset
            else:
                if existing:
                    update existing dataset with directory
                else:
                    add file to 'missing'
    elif home:
        files: if not in ignore list: add to 'home'
        directories:
            if in ignore list: ignore
            else traverse:
                directories:
                    if handler defined: use handler
                    else add as dataset
                files:
                    if handler defined: use handler
                    else add to 'home' dataset
    else:
        add all to 'other'

    '''

    frames_ignore_paths = ['crystalpics', 'diffpics']

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

    def __init__(self, squashfile, ns):
        self.epn = DatafileParameterSet.objects.get(
            datafile=squashfile,
            schema__namespace=ns
        ).datafileparameter_set.get(
            name__name='EPN'
        ).string_value

        exp_ns = 'http://www.tardis.edu.au/schemas/as/experiment/2010/09/21'
        parameter = ExperimentParameter.objects.get(
            name__name='EPN',
            name__schema__namespace=exp_ns,
            string_value=self.epn)
        self.experiment = parameter.parameterset.experiment
        self.s_box = get_or_create_storage_box(squashfile)
        self.metadata = get_squashfs_metadata(self.s_box)

        self.sq_inst = self.s_box.get_initialised_storage_instance()

    def parse(self):
        top = '.'
        dirnames, filenames = self.listdir('.')
        def_dataset = self.get_or_create_dataset('other files')
        result = self.add_files(top, filenames, def_dataset)
        for dirname in dirnames:
            if dirname == 'frames':
                result = result and self.parse_frames()
            elif dirname == 'home':
                result = result and self.parse_home()
        return result

    def parse_frames(self):
        '''
        add calibration frames to calibration dataset
        add all other files without changes
        '''
        top = 'frames'
        dirnames, filenames = self.listdir(top)
        result = self.add_files(top, filenames)
        if 'calibration' in dirnames:
            cal_dataset = self.get_or_create_dataset(
                'calibration', os.path.join(top, 'calibration'))
            result = result and self.add_subdir(top, cal_dataset)
            dirnames.remove('calibration')
        return result and all([self.add_subdir(os.path.join(top, dirname),
                                               ignore=self.frames_ignore_paths)
                               for dirname in dirnames])

    def parse_home(self):
        top = 'home'
        dirnames, filenames = self.listdir(top)
        home_dataset = self.get_or_create_dataset('home folder', top)
        result = self.add_files(top, filenames, home_dataset)
        for dirname in set(dirnames) & set(self.typical_home.keys()):
            if self.typical_home['dirname']['ignore']:
                continue
            dataset = self.get_or_create_dataset(
                self.typical_home['description'],
                os.path.join(top, dirname))
            result = result and self.add_subdir(os.path.join(top, dirname),
                                                dataset)
        for dirname in set(dirnames) - set(self.typical_home.keys()):
            result = result and self.parse_user_dir(dirname)
        return result

    def parse_user_dir(self, userdir):
        top = os.path.join('home', userdir)
        dirnames, filenames = self.listdir(top)
        user_dataset = self.get_or_create_dataset(
            'home/%s' % userdir, top)
        result = self.add_files(top, filenames, user_dataset)
        if 'auto' in dirnames:
            result = result and self.parse_auto_processing(userdir)
            dirnames.remove('auto')
        return result and all([
            self.add_subdir(os.path.join(top, dirname), user_dataset)
            for dirname in dirnames])

    def parse_auto_processing(self, userdir):
        top = os.path.join('home', userdir, 'auto')



    def add_file(self, top, filename, dataset=None):
        if self.find_existing_dfo(top, filename):
            return True
        else:
            return self.create_dfo(top, filename, dataset)

    def add_files(self, top, filenames, dataset=None):
        return all([self.add_file(top, filename, dataset)
                    for filename in filenames])

    def add_subdir(self, subdir, dataset=None, ignore=None):
        '''
        add a subdirectory and all children
        ignore folders that are defined in the ignore list
        '''
        dirnames, filenames = self.listdir(subdir)
        if ignore is not None:
            for path in ignore:
                if path in dirnames:
                    dirnames.remove(path)
        result = all([self.add_file(subdir, filename, dataset)
                      for filename in filenames])
        return result and all([self.add_subdir(dirname, dataset)
                               for dirname in dirnames])

    def create_dfo(self, top, filename, dataset=None):
        '''
        create dfo and datafile if necessary
        '''
        df, df_data = self.find_datafile(top, filename)
        if df:
            if df.dataset != dataset:
                df.dataset = dataset
                df.save()
            update_dataset(df.dataset, top)
        else:
            if dataset is None:
                dataset = self.get_or_create_dataset('lost and found')
            df = DataFile(
                dataset=dataset,
                filename=filename,
                directory=top,
                **df_data)
            df.save()
        dfo = DataFileObject(
            datafile=df,
            storage_box=self.s_box,
            uri=os.path.join(top, filename)
        )
        dfo.save()
        return True

    def find_datafile(self, top, filename):
        fullpath = os.path.join(top, filename)
        # df_data usually is {md5, sha512, size, mimetype_buffer}
        df_data = self.get_file_details(
            top, filename)
        try:
            existing_dfs = DataFile.objects.filter(
                filename=filename,
                md5sum=df_data['md5'],
                size=df_data['size'],
                dataset__experiments=self.experiment)
            nodir = existing_dfs.filter(directory='')
            samedir = existing_dfs.filter(directory=top)
            if nodir.count() == 1:
                existing_df = nodir[0]
                existing_df.directory = top
                existing_df.save()
            elif samedir.count() == 1:
                existing_df = samedir[0]
            else:
                existing_df = None
        except DataFile.DoesNotExist:
            existing_df = None
        df_data.update({
            'created_time': self.sq_inst.created_time(fullpath),
            'modification_time': self.sq_inst.modified_time(fullpath),
            # 'modified_time' is more standard, but will stick with df model
        })
        return existing_df, df_data

    def find_existing_dfo(self, top, filename):
        try:
            dfo = DataFileObject.objects.get(
                storage_box=self.s_box,
                uri=os.path.join(top, filename),
                datafile__dataset__experiments=self.experiment)
        except DataFileObject.DoesNotExist:
            dfo = False
        if dfo:
            update_dataset(dfo.datafile.dataset, top)
            return True
        return False

    def get_file_details(self, top, filename):
        fullpath = os.path.join(top, filename)
        try:
            md5, sha512, size, mimetype_buffer = generate_file_checksums(
                self.sq_inst.open(fullpath))
        except IOError as e:
            log.debug('squash parse error')
            log.debug(e)
            if os.path.islink(self.sq_inst.path(fullpath)):
                return {}
            raise
        mimetype = Magic(mime=True).from_buffer(mimetype_buffer or '')
        return {'size': str(size),
                'mimetype': mimetype,
                'md5sum': md5,
                'sha512sum': sha512}

    def get_or_create_dataset(self, name, top=None):
        '''
        returns existing or created dataset given a name

        returns False if the dataset is not unique by name

        top is the directory
        '''
        ds = Dataset.objects.filter(
            description=name, experiments=self.experiment)
        if len(ds) == 1:
            return ds[0]
        elif len(ds) > 1:
            return False
        ds = Dataset(description=name)
        if top is not None:
            ds.directory = top
        ds.save()
        ds.experiments.add(self.experiment)
        return ds

    def listdir(self, top):
        try:
            dirnames, filenames = self.sq_inst.listdir(top)
        except os.error as err:
            log.debug(err)
            return [], []
        dirnames = [d for d in dirnames if not d.startswith('.')]
        filenames = [f for f in filenames if not f.startswith('.')]
        return dirnames, filenames


def parse_squashfs_file(squashfile, ns):
    '''
    parse Australian Synchrotron specific SquashFS archive files
    '''

    parser = ASSquashParser(squashfile, ns)
    return parser.parse()
