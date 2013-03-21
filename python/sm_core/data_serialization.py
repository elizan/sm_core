#Copyright 2013 Thomas A Caswell
#tcaswell@uchicago.edu
#http://jfi.uchicago.edu/~tcaswell
#
#This program is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 3 of the License, or (at
#your option) any later version.
#
#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with this program; if not, see <http://www.gnu.org/licenses>.

import h5py


class SM_serial(object):
    '''
    A class to abstract away dealing with the hdf files.

    This class is meant to be a reference implementation for the
    interface for dealing with the hdf5 files.  That is, this in the
    _minimum_ functionality that must be provided for each language we
    want to support, and what people can assume will be available when
    developing new code.

    '''
    def format_frame_name(cls, N):
        '''
        Formats the name for the group that goes with frame N
        '''
        return 'frame_{0:07d}'

    def __init__(fname, fmode=None):
        '''
        :param fname: string, full path to the file to open
        :param fmode: in the subset 'r', 'rw', 'w+', 'w'
           r: read only, error if does not exist
           rw: read/write, error if does not exist
           w+: rw, will create if it does not exist
           w: create new file, WILL DELETE IF DOES EXIST

        It can be argued that this work should really be done in a
        class function, and then have the __init__ function take a
        h5py like object.
        '''
        self._file = h5py.File(fname, fmode)  # modulo patching up fmode
        pass

    def __del__(self):
        self._file.close()

    def loads(self, frame_num, data_set):
        '''
        :param frame_num: The number of the frame to get the data from
        :param data_set: a string that is the name of the data set to get
        :rtype:  `numpy.ndarray`

        returns the data in the given frame and data set

        Raises a sensible exception if data set does not exist or the
        frame number is out of range
        '''
        return self._file[self.format_frame_name(frame_num)][data_set][:]

    def dumps(self, frame_num, data_set, data, meta_data=None, over_write=False):
        '''
        :param frame_num: the frame to insert the data into
        :param data_set: string like name of the data set to store
        :param data: an `numpy.ndarray` of the data to store
        :param meta_data: a `dict` like object full of meta-data to be stored
        :param overwrite: if existing data should be over written, raises exception if file is read-only

        A function that dumps data to disk.  The meta-data is associated with the data set.
        '''
        # this needs to make sure the file is never left in a bad state
        pass

    def set_frame_md(self, frame_num, meta_data, over_write=False):
        '''
        :param frame_num: frame number
        :param meta_data: dictionary of meta-data
        :type meta_data: `dict`
        :param over_write: if existing meta-data will be over written, will raise error if True and file is read-only
        :type over_write: `bool`

        Set frame level meta-data.

        Raises sensible error if frame does not exist.  Raises sensible error if trying to set data that
        exists and `over_write==False`
        '''
        pass

    def get_frame_md(self, frame_num):
        '''
        :param frame_num: frame number
        :rtype: `dict`

        Returns the meta-data dictionary for the given frame
        '''
        pass

    def get_dset_md(self, frame_num, dset_name):
        '''
        :param frame_num: frame number
        :param dset_name: the name of the data set to get the md for
        :rtype: `dict`

        Returns the meta-data dictionary for the given dset in the given frame
        '''
        pass

    def list_dsets(self, frame_num):
        '''
        :param frame_num: frame number

        Returns a list of the data sets in the given frame number
        '''
        pass
