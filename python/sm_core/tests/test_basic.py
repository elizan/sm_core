#Copyright 2013 Thomas A Caswell
#tcaswell@uchicago.edu
#http://jfi.uchicago.edu/~tcaswell
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#
#1. Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#2. Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
#ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#The views and conclusions contained in the software and documentation are those
#of the authors and should not be interpreted as representing official policies,
#either expressed or implied, of the FreeBSD Project.
#

import string
import random
import infra
import numpy as np
from contextlib import closing
from sm_core import data_serialization as ds
import os


def _roundtrip_no_md(test_data):
    N = 6   # parameter for random name
    M = 10  # number of frames to dump
    with infra.path_provider() as base_path:
        # hacky version of generating a random name
        tmp_fname = os.path.join(base_path,
                                 ''.join(('test_roundtrip_',
                                         ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N)),
                                         '.h5')))

        with closing(ds.SM_serial(tmp_fname, 'w')) as test_sms:
            for dset_name, dtype, data in test_data:
                for k in range(M):
                    test_sms.dumps(k, dset_name, data)

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            for dset_name, dtype, data in test_data:
                for k in range(M):
                    read_data = test_sms.loads(k, dset_name)
                    assert read_data.dtype == dtype
                    assert np.all(read_data == data)


def test_int_round_trip():
    # generate datasets with all the int types
    test_data = [('test_data{:d}'.format(j),
                  np.dtype('int{:d}'.format(j)),
                  np.arange(50, dtype=np.dtype('int{:d}'.format(j))))
                 for j in (8, 16, 32, 64)]
    _roundtrip_no_md(test_data)


def test_float_round_trip():
    # generate datasets with float types.  float16 seems to not be supported by
    # someplace in the hdf/h5py stack
    test_data = [('test_data{:d}'.format(j),
                  np.dtype('float{:d}'.format(j)),
                  np.arange(50, dtype=np.dtype('float{:d}'.format(j))))
                 for j in (32, 64)]
    _roundtrip_no_md(test_data)


def test_uint_round_trip():
    # generate datasets with all the uint types
    test_data = [('test_data{:d}'.format(j),
                  np.dtype('uint{:d}'.format(j)),
                  np.arange(50, dtype=np.dtype('uint{:d}'.format(j))))
                 for j in (8, 16, 32, 64)]
    _roundtrip_no_md(test_data)


def test_complex_round_trip():
    # generate datasets with all the complex types
    test_data = [('test_data{:d}'.format(j),
                  np.dtype('complex{:d}'.format(j)),
                  np.arange(50, dtype=np.dtype('complex{:d}'.format(j))))
                 for j in (64, 128)]
    _roundtrip_no_md(test_data)


def test_frame_md():
    N = 6
    md_test = {'int': 1,
               'float': 1.0,
               'complex': 1 + 1j,
               'numpy': np.arange(5),
               'string': 'abc',
               'list': range(5)}

    with infra.path_provider() as base_path:
        # hacky version of generating a random name
        tmp_fname = os.path.join(base_path,
                                 ''.join(('test_int_roundtrip_',
                                         ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N)),
                                         '.h5')))

        with closing(ds.SM_serial(tmp_fname, 'w')) as test_sms:
            test_sms.set_frame_md(0, md_test)

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            read_md = test_sms.get_frame_md(0)
            assert [read_md[k] == md_test[k] for k in md_test.keys()]


def test_dset_md():
    N = 6
    md_test = {'int': 1,
               'float': 1.0,
               'complex': 1 + 1j,
               'numpy': np.arange(5),
               'string': 'abc'}

    with infra.path_provider() as base_path:
        # hacky version of generating a random name
        tmp_fname = os.path.join(base_path,
                                 ''.join(('test_md_roundtrip_',
                                         ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N)),
                                         '.h5')))
        test_data = range(50)
        dset_name = 'test'
        with closing(ds.SM_serial(tmp_fname, 'w')) as test_sms:
            test_sms.dumps(0, dset_name, test_data, meta_data=md_test)

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            read_md = test_sms.get_dset_md(0, dset_name)
            print read_md
            assert [read_md[k] == md_test[k] for k in md_test.keys()]


def test_dset_md_update():
    N = 6
    md_test = {'int': 1,
               'float': 1.0,
               'complex': 1 + 1j,
               'numpy': np.arange(5),
               'string': 'abc'}

    md_test2 = {'int': 2,
                'float': 2.0,
                'complex': 2 + 1j,
                'numpy': np.arange(5) * 2,
                'string': 'def'}

    with infra.path_provider() as base_path:
        # hacky version of generating a random name
        tmp_fname = os.path.join(base_path,
                                 ''.join(('test_md_roundtrip_',
                                         ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N)),
                                         '.h5')))
        test_data = range(50)
        dset_name = 'test'
        with closing(ds.SM_serial(tmp_fname, 'w')) as test_sms:
            test_sms.dumps(0, dset_name, test_data, meta_data=md_test)

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            read_md = test_sms.get_dset_md(0, dset_name)
            print read_md
            assert [read_md[k] == md_test[k] for k in md_test.keys()]

        with closing(ds.SM_serial(tmp_fname, 'r+')) as test_sms:
            test_sms.update_dset_md(0, dset_name, meta_data=md_test2, over_write=True)

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            read_md = test_sms.get_dset_md(0, dset_name)
            print read_md
            assert [read_md[k] == md_test[k] for k in md_test2.keys()]


def test_dset_md_update_fail():
    N = 6
    md_test = {'int': 1,
               'float': 1.0,
               'complex': 1 + 1j,
               'numpy': np.arange(5),
               'string': 'abc'}

    md_test2 = {'int': 2,
                'float': 2.0,
                'complex': 2 + 1j,
                'numpy': np.arange(5) * 2,
                'string': 'def'}

    with infra.path_provider() as base_path:
        # hacky version of generating a random name
        tmp_fname = os.path.join(base_path,
                                 ''.join(('test_md_roundtrip_',
                                         ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N)),
                                         '.h5')))
        test_data = range(50)
        dset_name = 'test'
        with closing(ds.SM_serial(tmp_fname, 'w')) as test_sms:
            test_sms.dumps(0, dset_name, test_data, meta_data=md_test)

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            read_md = test_sms.get_dset_md(0, dset_name)
            print read_md
            assert [read_md[k] == md_test[k] for k in md_test.keys()]

        with closing(ds.SM_serial(tmp_fname, 'r+')) as test_sms:
            try:
                test_sms.update_dset_md(0, dset_name, meta_data=md_test2, over_write=False)
                # this should fail and raise a RuntimeError
            except RuntimeError:
                pass
            else:
                # if this gets hit, something is wrong
                assert False

        with closing(ds.SM_serial(tmp_fname, 'r')) as test_sms:
            read_md = test_sms.get_dset_md(0, dset_name)
            print read_md
            assert [read_md[k] == md_test[k] for k in md_test.keys()]
