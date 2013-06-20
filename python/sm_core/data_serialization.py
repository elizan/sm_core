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
import h5py
import numpy as np


class SM_serial(object):
    '''
    A class to abstract away dealing with the hdf files.

    This class is meant to be a reference implementation for the
    interface for dealing with the hdf5 files.  That is, this is the
    *minimum* functionality that must be provided for each language we
    want to support, and what people can assume will be available when
    developing new code.

    '''
    _VALID_FILE_MODES = {'r', 'r+', 'w', 'w-', 'a'}   #: valid file modes

    def _format_frame_name(self, N):
        '''Private function to format the name for the
        group that goes with frame N

        Parameters
        ----------
        N : int
            The frame number

        Returns
        -------
        ret : :py:class:`str`
            Properly formatted string
        '''
        return 'time_{0:07d}'.format(N)

    @classmethod
    def open(cls, fname, fmode):
        """
        Parameters
        ----------
        fname : :py:class:`str`
            full path to the file to open
        fmode : :py:class:`str` or :py:class:`None`
           in the set {'r', 'r+', 'w', 'w-', 'a'}


           ===  ================================================
            r   Readonly, file must exist
            r+  Read/write, file must exist
            w   Create file, truncate if exists
            w-  Create file, fail if exists
            a   Read/write if exists, create otherwise (default)
           ===  ================================================


           Defaults to 'a'
        """

        if fmode is None:
            fmode = 'a'

        # TODO add brains to keep track if the objcet is writable and raise
        # reasonable errors
        if fmode not in cls._VALID_FILE_MODES:
            print "invalid mode, converting to 'a'"
            fmode = 'a'
        _file = h5py.File(fname, fmode)  # modulo patching up fmode

        write_flag = fmode != 'r'
        return cls(_file, write_flag)

    def __init__(self, file_obj, write_flg):
        '''Init function.  You should use the py:func:`open` class method.

        Parameters
        ----------
        file : `h5py.File`
            `h5py.File` object to use and the backing store
        write_flg: `bool`
            if the backing file is write-able

        '''
        self._file = file_obj
        self._write = write_flg
        self._open = True

    def __del__(self):
        self.close()

    def close(self):
        '''Closes backing file
        '''
        # sort out if we need to track the open/close state
        #of the file to raise sensible errors
        if self._open:
            self._file.close()
            self._open = False

    def loads(self, frame_num, data_set):
        '''Reads the given data set from the given frame.

        Parameters
        ----------
        frame_num : int
            The number of the frame to get the data from
        data_set : :py:class:`str`
            a string that is the name of the data set to get

        Returns
        -------
        ret :  :py:class:`~numpy.ndarray`
            data is dataset
        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        # TODO add error checking so the raw h5 errors don't propagate up
        return self._file[self._format_frame_name(frame_num)][data_set][:]

    def dumps(self, frame_num, data_set, data, meta_data=None, over_write=False, **kwargs):
        '''Adds data to the file.  The meta-data is associated with the data set.

        additional kwargs are passed to backing structure

        Parameters
        ----------
        frame_num : int
            the frame to insert the data into
        data_set : :py:class:`str`
            name of the data set to store
        data : :py:class:`~numpy.ndarray`
            the data to store
        meta_data : :py:class:`dict` like or :py:class:`None`
            meta-data to be stored with the data set
        overwrite : bool
            if existing data should be over written, defaults to False
        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        if not self._write:
            raise RuntimeError("trying to write to a read-only file")

        # this needs to make sure the file is never left in a bad state
        data = np.asarray(data)
        grp = self._require_grp(self._format_frame_name(frame_num))
        try:
            dset = grp[data_set]
        except KeyError:
            # this is the main behavior, it creates data set
            dset = grp.create_dataset(data_set, data=data, **kwargs)
        else:
            if over_write:
                if not isinstance(dset, h5py._hl.dataset.Dataset):
                    # TODO use custom class for this exception
                    raise RuntimeError("there is a group (not a dataset) where the data set needs to go."
                                       "Check names and that file is valid")
                # delete the existing data set
                del grp[data_set]
                dset = grp.create_dataset(data_set, data=data, **kwargs)
        if meta_data:
            # dump the meta-data
            for key, value in meta_data.items():
                dset.attrs[key] = value

    def update_dset_md(self, frame_num, dset_name, meta_data, over_write=False):
        '''Update the meta-data on a dataset.

        Parameters
        ----------
        frame_num : int
           frame number
        dset_name : :py:class:`str`
            name of the dataset
        meta_data : :py:class:`dict` like
            The meta-data to set
        over_write : bool
            if existing meta-data will be over written, will raise error if True and file is read-only
        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        if not self._write:
            raise RuntimeError("trying to write to a read-only file")

        grp = self._require_grp(self._format_frame_name(frame_num))
        _object_set_md(grp[dset_name], meta_data, over_write)

    def set_frame_md(self, frame_num, meta_data, over_write=False):
        '''Set frame level meta-data.  Will create frame if it does not exist

        Parameters
        ----------
        frame_num : int
            frame number
        meta_data : :py:class:`dict` like
            The meta-data to set
        over_write : bool
            if existing meta-data will be over written, will raise error if True and file is read-only
        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        if not self._write:
            raise RuntimeError("trying to write to a read-only file")

        grp = self._require_grp(self._format_frame_name(frame_num))
        _object_set_md(grp, meta_data, over_write)

    def get_frame_md(self, frame_num):
        '''Returns the meta-data dictionary for the given frame

        Parameters
        ----------
        frame_num : int
            frame number

        Returns
        -------
            md : :py:class:`dict`

        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        #TODO make error messages helpful

        grp = self._open_group(self._format_frame_name(frame_num))
        return dict(grp.attrs.iteritems())

    def get_dset_md(self, frame_num, dset_name):
        '''Returns the meta-data dictionary for the given dset in the given frame

        Parameters
        ----------
        frame_num : int
            frame number

        dset_name : :py:class:`str`
            Name of the data set

        Returns
        -------
            md : :py:class:`dict`

        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        grp = self._open_group(self._format_frame_name(frame_num))
        dset = grp[dset_name]
        return dict(dset.attrs.iteritems())

    def list_dsets(self, frame_num):
        '''Returns a list of the data sets in the given frame number

        Parameters
        ----------
        frame_num : int
             frame number


        '''

        if not self._open:
            raise RuntimeError("Trying to operate on a closed file")
        grp = self._open_group(self._format_frame_name(frame_num))
        return _subgroup_recurse(grp, '')

    def _require_grp(self, path):
        """Private function to handle requiring that a group exists.
        Returns the existing group it if exists, creates and returns
        the group if it does not.

        Should only be called in function which expect a writable file

        Parameters
        ----------
        path : :py:class:`str`
            The absolute path of the group to open/create

        Returns
        -------
        grp : `~h5py._hl.group.Group`
            a valid group object at `path`

        """

        try:
            grp = self._file.require_group(path)
        except TypeError as e:
            # TODO launder this through a custom error class
            print ("A non-group exists where at" +
                   "Are you sure that this is a properly formatted file?" +
                   "File : {0}".format(self._file.filename))
            raise e

        return grp

    def _open_group(self, path):
        """Private function to handle opening a group.
        Returns the group if it exists, raises
        error if it does not.

        Parameters
        ----------
        path : :py:class:`str`
            The absolute path of the group to open/create

        Returns
        -------
        grp : `~h5py._hl.group.Group`
            a valid group object at `path`

        """

        try:
            grp = self._file[path]
        except KeyError as e:
            # TODO launder this through a custom error class
            print ("The group does not exist" +
                   "Are you sure that this is a properly formatted file?" +
                   "File: {0}".format(self._file.filename))
            raise e

        if not isinstance(grp, h5py._hl.group.Group):
            raise RuntimeError("The object found is not a group")
        return grp


def _object_set_md(obj, meta_data, over_write):
    """Private function for setting meta-data

    Parameters
    ----------

    obj : `~h5py.File`, `~h5py.Group`, or `~h5py.Dataset` object
        The object to set the meta data of
    meta_data : :py:class:`dict` like
        The data to be set, must only contain types that play nice with `hdf`
    over_write : bool
        If existing data should be over-written
    """
    existing_keys = set(obj.attrs.keys())

    if ((not over_write) and any(k in existing_keys
                                 for k in meta_data.keys())):
        raise RuntimeError("trying to over-write an existing key")
    for key, value in meta_data.items():
        obj.attrs[key] = value


def _subgroup_recurse(base_object, base_path):
    """
    Private function for finding all the data sets under a given group.

    Parameters
    ----------

    base_object : `~h5py.File`, `~h5py.Group`, or `~h5py.Dataset` object
        The object to look for data sets in
    base_path : :py:class:`str`
        Relative path of the base object relative to where the search started

    Returns
    -------
    name_list : `list`
        list of relative paths of all data sets under the base object
    """
    name_list = []
    for key in base_object.keys():
        obj = base_object[key]
        if isinstance(obj, h5py._hl.dataset.Dataset):
            name_list.append(base_path + '/' + key)
        elif isinstance(obj, h5py._hl.group.Group):
            name_list.extend(_subgroup_recurse(obj, base_path + '/' + key))
    return name_list
