#include <DataFrame/MMap/ObjectVector.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <time.h>

using namespace std;
using namespace hmdf;

// ----------------------------------------------------------------------------

class  data_class  {

public:

    typedef unsigned int    size_type;
    typedef time_t          time_type;

    inline data_class ()  { name [0] = 0; }
    inline data_class (const data_class &that)  { *this = that; }

    inline data_class &operator = (const data_class &rhs)  {

        if (this != &rhs)  {
            strcpy (name, rhs.name);
            i = rhs.i;
            counter = rhs.counter;
            d = rhs.d;
        }

        return (*this);
    }

    bool operator == (const data_class &rhs) const  {

        return (! strcmp (name, rhs.name) &&
                i == rhs.i &&
                counter == rhs.counter &&
                d == rhs.d);
    }
    bool operator != (const data_class &rhs) const { return(! (*this == rhs)); }

    void print () const  {

        cout << "Printing from data_class::print()\n"
             << "\tname    = " << name << "\n"
             << "\ti       = " << i << "\n"
             << "\tcounter = " << counter << "\n"
             << "\td       = " << d << "\n" << endl;

        return;
    }

    char    name [64];
    int     i { 0 };
    size_t  counter { 0 };
    double  d { 23.33 };
};

// ----------------------------------------------------------------------------

static bool operator < (const data_class &lhs, const data_class &rhs)  {

    return (lhs.counter < rhs.counter);
}

// ----------------------------------------------------------------------------

#ifndef _WIN32

#include <unistd.h>

typedef MMapVector<data_class>  MyObjBase;
typedef std::vector<data_class> MyStdVec;
// typedef ObjectVector<data_class, MMapSharedMem> MyObjBase;

static const size_t ITER_COUNT = 10000;
static const char   *OBJECT_BASE_NAME = "./testfile";
static const char   *OBJECT_BASE_NAME2 = "./testfile2";
static const char   *OBJECT_BASE_NAME3 = "./testfile3";
static const char   *OBJECT_BASE_NAME4 = "./testfile4";

#endif // _WIN32

// ----------------------------------------------------------------------------

int main (int argc, char *argv [])  {

#ifndef _WIN32

    cout.precision (5);

    MyObjBase   write_objbase (OBJECT_BASE_NAME);
    MyStdVec    stdvec;

    srand (5);
    srand48 (5);

    write_objbase.reserve (ITER_COUNT * 2);
    for (size_t i = 0; i < ITER_COUNT; ++i)  {
        const int           the_int = rand ();
        const double        the_double = drand48 ();
        FixedSizeString<63> str;

        str.printf ("the number is %d", the_int);

        data_class  dc;

        strcpy (dc.name, str.c_str ());
        dc.i = the_int;
        dc.counter = i;
        dc.d = the_double;

        write_objbase.push_back (dc);
        stdvec.push_back (dc);
        if (i % 1000 == 0)
            cout << "Inserted record number " << i << endl;
    }
    write_objbase.shrink_to_fit();
    cout << "Inserted total of " << write_objbase.size ()
         << " records" << endl;
    cout << endl;
    write_objbase.set_access_mode (ACCESS_MODE::random);

    const MyObjBase read_objbase (OBJECT_BASE_NAME);
    MyObjBase       read_objbase2 (read_objbase);

    assert(write_objbase == read_objbase);
    assert(stdvec == read_objbase);
    assert(stdvec == read_objbase2);
    assert(write_objbase == stdvec);
    stdvec[5].i = -8;
    assert(write_objbase != stdvec);

    MyObjBase read_objbase3 (OBJECT_BASE_NAME2, stdvec);

    assert(stdvec == read_objbase3);

    srand (5);
    srand48 (5);

    cout << "\nTesting the print function:\n";
    read_objbase.back ().print ();
    cout << "Print function was successfull\n" << endl;

    if (read_objbase.size () != write_objbase.size ())  {
        cout << "Object count of write file: "
             << write_objbase.size ()
             << " doesn't match the object count of read file: "
             << read_objbase.size () << endl;
        return (EXIT_FAILURE);
    }

    for (size_t i = 0; i < ITER_COUNT; ++i)  {
        const int           the_int = rand ();
        const double        the_double = drand48 ();
        FixedSizeString<63> str;

        str.printf ("the number is %d", the_int);

        data_class  dc;
        data_class  read_dc;

        strcpy (dc.name, str.c_str ());
        dc.i = the_int;
        dc.counter = i;
        dc.d = the_double;

        read_dc = read_objbase[i];
        if (! (dc == read_dc))  {
            cout << "Something doesn't add up:\n"
                 << "\tdc.name = '" << dc.name << "',  "
                 << "dc.i = '" << dc.i << "',  "
                 << "dc.counter = '" << dc.counter << "',  "
                 << "dc.d = '" << dc.d << "'\n"
                 << "\tread_dc.name = '" << read_dc.name << "',  "
                 << "read_dc.i = '" << read_dc.i << "',  "
                 << "read_dc.counter = '" << read_dc.counter << "',  "
                 << "read_dc.d = '" << read_dc.d << "'\n"
                 << endl;

            return (EXIT_FAILURE);
        }
        // cout << "Read record number " << i << endl;
    }
    cout << "Read total of " << ITER_COUNT << " records" << endl;
    cout << endl;

    if (! (read_objbase [2] == write_objbase [2]))  {
        cout << "Some thing wrong with the call to [] operator:\n"
             << "\tread_objbase [2].name = '"
             << read_objbase [2].name << "',  "
             << "read_objbase [2].i = '" << read_objbase [2].i << "',  "
             << "read_objbase [2].counter = '"
             << read_objbase [2].counter << "',  "
             << "read_objbase [2].d = '" << read_objbase [2].d << "'\n"
             << "\twrite_objbase [2].name = '"
             << write_objbase [2].name << "',  "
             << "write_objbase [2].i = '" << write_objbase [2].i << "',  "
             << "write_objbase [2].counter = '"
             << write_objbase [2].counter << "',  "
             << "write_objbase [2].d = '" << write_objbase [2].d << "'\n"
             << endl;

            return (EXIT_FAILURE);
    }
    cout << "Compared read_objbase [2] == write_objbase [2]" << endl;

    const MyObjBase c_read_objbase (OBJECT_BASE_NAME);

    if (! (c_read_objbase [3] == write_objbase [3]))  {
        cout << "Some thing wrong with the call to [] operator:\n"
             << "\tc_read_objbase [2].name = '"
             << c_read_objbase [2].name << "',  "
             << "c_read_objbase [2].i = '" << c_read_objbase [2].i << "',  "
             << "c_read_objbase [2].counter = '"
             << c_read_objbase [2].counter << "',  "
             << "c_read_objbase [2].d = '" << c_read_objbase [2].d << "'\n"
             << "\twrite_objbase [2].name = '"
             << write_objbase [2].name << "',  "
             << "write_objbase [2].i = '" << write_objbase [2].i << "',  "
             << "write_objbase [2].counter = '"
             << write_objbase [2].counter << "',  "
             << "write_objbase [2].d = '" << write_objbase [2].d << "'\n"
             << endl;

            return (EXIT_FAILURE);
    }
    cout << "Compared c_read_objbase [2] == write_objbase [2]" << endl;
    cout << endl;

    cout << "Object count of write_objbase: "
         << write_objbase.size () << endl;
    cout << "Object count of read_objbase: "
         << read_objbase.size () << endl;
    cout << "Object count of c_read_objbase: "
         << c_read_objbase.size () << endl;
    cout << endl;

    time_t  ct = 0;

    ct = write_objbase.creation_time ();
    cout << "Creation_time of write_objbase: " << ctime (&ct);
    ct = read_objbase.creation_time ();
    cout << "Creation_time of read_objbase: " << ctime (&ct);
    ct = c_read_objbase.creation_time ();
    cout << "Creation_time of c_read_objbase: " << ctime (&ct);
    cout << endl << endl;

    cout << "\nTesting the forward iterators:\n\n";

    for (MyObjBase::const_iterator citr = write_objbase.begin ();
         citr != write_objbase.end (); ++citr)  {
        if (citr->counter % 1000 == 0)
            cout << "\t" << citr->counter << endl;
    }

    cout << "\nTesting the reverse iterators:\n\n";

    for (MyObjBase::const_reverse_iterator critr = write_objbase.rbegin ();
         critr != write_objbase.rend (); ++critr)  {
        if (critr->counter % 1000 == 0)
            cout << "\t" << critr->counter << endl;
    }

   // Just checking if they compile
   //
    const MyObjBase::iterator               itr = write_objbase.begin ();
    const MyObjBase::const_iterator         citr = itr;
    const MyObjBase::const_reverse_iterator critr = citr;
    const MyObjBase::reverse_iterator       ritr = itr;

    MyObjBase obj_vector1 (OBJECT_BASE_NAME3, 100);

    assert(obj_vector1.size() == 100);
    assert(obj_vector1[0].i == 0);
    assert(obj_vector1[0].d == 23.33);
    assert(obj_vector1[48].i == 0);
    assert(obj_vector1[48].d == 23.33);
    assert(obj_vector1[99].i == 0);
    assert(obj_vector1[99].d == 23.33);

    MyObjBase obj_vector2 (OBJECT_BASE_NAME4, stdvec.begin(), stdvec.end());

    assert(obj_vector2.size() == stdvec.size());
    assert(obj_vector2[0].i == stdvec[0].i);
    assert(obj_vector2[0].d == stdvec[0].d);
    assert(obj_vector2[48].i == stdvec[48].i);
    assert(obj_vector2[48].d == stdvec[48].d);
    assert(obj_vector2[99].i == stdvec[99].i);
    assert(obj_vector2[99].d == stdvec[99].d);

    ::unlink(OBJECT_BASE_NAME);
    ::unlink(OBJECT_BASE_NAME2);
    ::unlink(OBJECT_BASE_NAME3);
    ::unlink(OBJECT_BASE_NAME4);

#endif // _WIN32

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
