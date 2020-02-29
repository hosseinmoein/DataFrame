// Hossein Moein
// May 29, 2019
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <DataFrame/MMap/MMapFile.h>
#include <DataFrame/MMap/MMapSharedMem.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>

using namespace std;
#ifndef _WIN32
using namespace hmdf;
#endif // _WIN32

static const size_t         SHARED_MEMORY_NUM = 100;
static const size_t         EVEN_SIZE = 1000;
static const size_t         ODD_SIZE = 100000;
static const double         DOUBLE_VALUE = 1.123456789;
static const long long int  LONGLONG_VALUE = 1234567890LL;

//-----------------------------------------------------------------------------

int main (int argc, char *argv [])  {

#ifndef _WIN32

    MMapSharedMem   *sh_files [SHARED_MEMORY_NUM];
    String1K        str;

    for (size_t i = 0; i < SHARED_MEMORY_NUM; ++i)  {
        str.printf ("/test%d", i);

        sh_files [i] =
            new MMapSharedMem (str.c_str (), MMapSharedMem::_bwrite_, 10000);
        // sh_files [i] = new MMapSharedMem (str.c_str (),
        //                                   MMapSharedMem::_write_,
        //                                   10000);
        sh_files [i]->set_precision (15);
        cout << "Opened shared memory " << i << " successfully." << endl;
    }
    cout << endl << endl << endl;

    struct timespec rqt;

    rqt.tv_sec = 5;
    rqt.tv_nsec = 0;
    nanosleep (&rqt, nullptr);

    cout << "Starting to write" << endl;
    for (size_t i = 0; i < SHARED_MEMORY_NUM; ++i)  {
        if (i % 2)  {
            for (size_t j = 0; j < ODD_SIZE; ++j)  {
                // (*(sh_files [i])) << DOUBLE_VALUE << "\n";
                sh_files [i]->write ((const void *) &DOUBLE_VALUE,
                                     sizeof (double), 1);
            }
        }
        else  {
            for (size_t j = 0; j < EVEN_SIZE; ++j)  {
                // (*(sh_files [i])) << LONGLONG_VALUE << "\n";
                 sh_files [i]->write ((const void *) &LONGLONG_VALUE,
                                      sizeof (long long int), 1);
            }
        }
        cout << "Wrote to shared memory " << i << " successfully." << endl;
    }
    cout << endl << endl << endl;

    nanosleep (&rqt, nullptr);

    cout << "Starting to Read" << endl;
    for (size_t i = 0; i < SHARED_MEMORY_NUM; ++i)  {
        sh_files [i]->rewind ();
        cout << "Rewound shared memory " << i << " successfully." << endl;
    }

    for (size_t i = 0; i < SHARED_MEMORY_NUM; ++i)  {
        if (i % 2)  {
            double  the_value;

            for (size_t j = 0; j < ODD_SIZE; ++j)  {
                (*(sh_files [i])) >> the_value;
                // sh_files [i]->read((void *) &the_value, sizeof (double), 1);

                if (the_value != DOUBLE_VALUE)  {
                    cerr << "ERROR: (" << ") " << the_value
                         << " != " << DOUBLE_VALUE
                         << " (" << i << "/" << j << ")" << endl;
                    return (-1);
                }
            }
        }
        else  {
            long long int   the_value;

            for (size_t j = 0; j < EVEN_SIZE; ++j)  {
                (*(sh_files [i])) >> the_value;
                // sh_files [i]->read ((void *) &the_value,
                //                      sizeof (long long int), 1);

                if (the_value != LONGLONG_VALUE)  {
                    cerr << "ERROR: (" << ") " << the_value
                         << " != " << LONGLONG_VALUE
                         << " (" << i << "/" << j << ")" << endl;
                    return (-1);
                }
            }
        }
        cout << "Compared shared memory " << i << " successfully." << endl;
    }
    cout << endl << endl << endl;

    nanosleep (&rqt, nullptr);

    cout << "Testing clobber() functionality" << endl;
    sh_files [0]->clobber ();
    sh_files [0]->open ();

    long long int   the_value;

    (*(sh_files [0])) >> the_value;
    cout << the_value << endl;
    (*(sh_files [0])) >> the_value;
    cout << the_value << endl;
    (*(sh_files [0])) >> the_value;
    cout << the_value << endl;

    cout << endl << endl << endl;
    cout << "Starting to delete" << endl;
    for (size_t i = 0; i < SHARED_MEMORY_NUM; ++i)  {
        sh_files [i]->unlink ();
        delete sh_files [i];
        cout << "Deleted shared memory " << i << " successfully." << endl;
    }

    cout << "Testing the MMap copying to Shared memory" << endl;

    MMapFile      mmap_file ("~/.bashrc", MMapFile::_read_);
    MMapSharedMem copied_sharedmem ("./MMapBase.cc",
                                    MMapSharedMem::_write_,
                                    mmap_file.get_file_size ());

    copied_sharedmem << mmap_file;

    if (! copied_sharedmem.is_ok ())  {
        cout << "ERROR copying mmap file to shared memory" << endl;
    }

#endif // _WIN32

    return (0);
}

//-----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
