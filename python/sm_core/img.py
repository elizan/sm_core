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


from __future__ import division
from abc import ABCMeta, abstractmethod

import os.path
import datetime
import xml.dom.minidom
import PIL.Image
import numpy as np
import re


class No_Such_Frame(Exception):
    '''
    An exception to be raised when the requested frame does not exist
    '''
    pass


class Image_Stream(object):
    '''
    An ABC for a stream of images.  This base class
    only requires that iteration be defined.
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def __iter__(self):
        '''
        `Image_Stream` objects must implement `__iter__`.

        Yields the data as a (M, N) ndarray or as a (M, N, C) ndarray
        '''
        while False:
            yield None

    @abstractproperty
    def frame_size(self):
        '''
        The dimensions of each frame.  It is assumed that _all_ frames
        in the stream are the same size.

        Returns
        -------
        dims : tuple
            A length d tuple where d is the dimension of the image
        '''
        return ()

    @abstractproperty
    def pixel_dtype(self):
        '''
        The data type of a single pixel.  Must be constant over stream.

        Returns
        -------
        dtype : np.dtype
            The data type of the pixel data.
        '''
        return None

    def channels(self):
        '''
        Returns the number of channels per pixel.  Must be constant over stream

        Returns
        -------
        channel_count : int
            The number of channels in each pixel
        '''
        return 0


class Image_Sequence(Image_Stream):
    '''Base class for wrapping image sequences'''
    def __getitem__(self, key):
        '''
        Magic to make slicing work correctly
        '''
        if type(key) == slice:
            return map(self.get_frame, range(self.image_count)[key])

        return self.get_frame(key)

    @abstractmethod
    def __len__(self):
        pass

    @abstractproperty
    def path(self):
        '''
        The path of the underlying data source

        Returns
        -------
        path : str
            The path of the underlying data type
        '''
        return ''

    @abstractmethod
    def get_frame(self, j):
        '''
        Get frame j from the sequence

        Parameters
        ----------
        j : int
            The frame to extract from the sequnce

        Returns
        -------
        img : np.ndarray
            An `np.ndarry` with
        '''
        raise NotImplementedError


class Stack_wrapper(Image_Stream):
    def __init__(self, fname):
        '''fname is the full path '''
        self.im = PIL.Image.open(fname)

        self.im.seek(0)
        # get image dimensions from the meta data the order is flipped
        # due to row major v col major ordering in tiffs and numpy
        self.im_sz = [self.im.tag[0x101][0],
                      self.im.tag[0x100][0]]
        self.cur = self.im.tell()

        j = 0
        while True:
            try:
                self.im.seek(j)
                j += 1
            except EOFError:
                self.im.seek(0)
                break

        self._len = j

    def __len__(self):
        return self._len

    def get_frame(self, j, cast_dtype=None):
        '''Extracts the jth frame from the image sequence.
        if the frame does not exist Raises No_such_frame exception'''

        try:
            self.im.seek(j)
        except EOFError:
            raise No_such_frame("There is no frame" +
                                " {} in {} which has length {}".format(j, self.path, len(self))

        self.cur = self.im.tell()
        data = np.reshape(self.im.getdata(), self.im_sz)
        if cast_dtype is not None:
            return data.astype(cast_dtype)

        return data


    def __iter__(self):
        self.im.seek(0)
        self.old = self.cur
        self.cur = self.im.tell()
        return self

    def next(self):
        try:
            self.im.seek(self.cur)
            self.cur = self.im.tell()+1
        except EOFError:
            self.im.seek(self.old)
            self.cur = self.im.tell()
            raise StopIteration
        return np.reshape(self.im.getdata(), self.im_sz)


class Stack_wrapper_mm(Stack_wrapper):
    def __init__(self, *args, **kwangs):
        Stack_wrapper.__init__(self)
        self._parse_xml_string = _parse_xml_string_mm

    def get_meta(self, j):
        '''
        Returns the meta-data associated with frame `j`, currently only works
        on MetaMorph stacks

        Parameters
        ----------
        j : int
            The frame to pull the meta-data for

        Returns
        -------
        md : dict
            dictionary of the meta-data
        '''
        cur = self.im.tell()
        if cur != j:
            self.im.seek(j)
            xml_str = self.im.tag[270]
            self.im.seek(cur)
        else:
            xml_str = self.im.tag[270]

        return self._parse_xml_string(xml_str)


class Series_wrapper(Image_wrapper_base):
    prog = re.compile(r'(.*?)([0-9]+)\.([a-zA-Z]+)')

    @classmethod
    def create_wrapper(cls, first_fname):
        '''
        :param first_fname: the full path to the first image in the series

        Uses regular expressions to guess the basename, offset, padding and extension. '''

        res = cls.prog.search(first_fname)
        if res is None:
            return None
        basename, num, ext = res.groups()
        padding = len(num)
        fmt_str = basename + '{:0' + str(padding) + '}.' + ext
        return cls(fmt_str, img_num_offset=int(num))

    def __init__(self, fmt_string, img_num_offset=0):
        '''
        :param base_name: the  the full path up to the numbering
        :param ext: file extension
        :param padding: the number of digits the numbers are padded to
        :param img_num_off_set: the number of the first frame (to map to the first frame = 0)

        '''
        self.base_name = fmt_string

        self._im_num_offset = img_num_offset
        j = 0
        while os.path.isfile(self.base_name.format(j+self._im_num_offset)):
            j += 1
        self._len = j

    def __len__(self):
        return self._len

    def get_frame(self, j):
        '''Extracts the jth (off set) frame from the image sequence.
        if the frame does not exist return None'''
        try:
            im = PIL.Image.open(self.base_name.format(j+self._im_num_offset))
        except IOError:
            print "didn't find the file"
            print self.base_name.format(j+self._im_num_offset)
            return None
        img_sz = im.size[::-1]
        return np.reshape(im.getdata(), img_sz).astype('uint16')


def _parse_xml_string_mm(xml_str):
    '''
    Parses the xml string generated by MetaMorph
    '''

    def _store(md_dict, name, val):
        if name == "acquisition-time-local" or name == "modification-time-local":
            tmp = int(val[18:])
            val = val[:18] + "%(#)03d" % {"#": tmp}
            val = datetime.datetime.strptime(val, '%Y%m%d %H:%M:%S.%f')
        md_dict[name] = val

    def _parse_attr(md_dict, dom_obj):
        if dom_obj.getAttribute("id") == "Description":
            _parse_des(md_dict, dom_obj)
        elif dom_obj.getAttribute("type") == "int":
            _store(md_dict, dom_obj.getAttribute("id"), int(dom_obj.getAttribute("value")))
        elif dom_obj.getAttribute("type") == "float":
            _store(md_dict, dom_obj.getAttribute("id"), float(dom_obj.getAttribute("value")))
        else:
            _store(md_dict, dom_obj.getAttribute("id"), dom_obj.getAttribute("value").encode('ascii'))

    def _parse_des(md_dict, des_obj):
        des_string = des_obj.getAttribute("value")
        des_split = des_string.split("&#13;&#10;")

        for x in des_split:
            tmp_split = x.split(":")
            if len(tmp_split) == 2:
                _store(md_dict, tmp_split[0], tmp_split[1].encode('ascii'))

    dom = xml.dom.minidom.parseString(xml_str)

    props = dom.getElementsByTagName("prop")
    f = dict()
    for p in props:
        _parse_attr(f, p)

    return f
