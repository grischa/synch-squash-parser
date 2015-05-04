import os

from celery.task import task

from tardis.tardis_portal.models import Experiment

from tardis.apps.synch_squash_parser.parser import parse_squashfs_file
from tardis.apps.synch_squash_parser import register_squashfile


def reset_status(dfid):
    df = DataFile.objects.get(id=dfid)
    ps = df.datafileparameterset_set.all()[0]
    ps.set_param('parse_status', 'incomplete')


@task(name='apps.synch_squash_parser.parse')
def parse(epn):
    namespace = 'http://synchrotron.org.au/mx/squashfsarchive/1'
    sq_df = register_squashfile(
        Experiment.objects.get(title="Experiment %s" % epn).id,
        epn,
        '/srv/rdsi-tape/squashstore',
        '%s.squashfs' % epn,
        namespace)
    return parse_squashfs_file(sq_df, namespace)
