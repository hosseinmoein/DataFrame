// Hossein Moein
// August 23, 2007

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

#include <time.h>
#include <strings.h>

#include <DMScu_FixedSizeString.h>

#include <DMSob_ObjectBase.h>
#include <DMSobRT_TimeSeries.h>
#include <DMSobRT_DatabaseOrganizer.h>

using namespace std;

// ----------------------------------------------------------------------------

class  header_class  {

    public:

        inline header_class () : i (0)  { name [0] = 0; unused [0] = 0; }
        inline header_class (const header_class &that)  { *this = that; }

        inline header_class &operator = (const header_class &rhs)  {

            if (this != &rhs)  {
                strcpy (name, rhs.name);
                i = rhs.i;
                strcpy (unused, rhs.unused);
            }

            return (*this);
        }

        bool operator == (const header_class &rhs) const  {

            return (! strcmp (name, rhs.name) && i == rhs.i);
        }

        char    name [64];
        int     i;
        char    unused [20];
};

// ----------------------------------------------------------------------------

class  data_class  {

    public:

        typedef unsigned int    size_type;
        typedef time_t          time_type;

        inline data_class () : i (0), counter (0), d (0.0)  { name [0] = 0; }
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

        void print () const  {

            cout << "Printing from data_class::print()\n"
                 << "\tname    = " << name << "\n"
                 << "\ti       = " << i << "\n"
                 << "\tcounter = " << counter << "\n"
                 << "\td       = " << d << "\n" << endl;

            return;
        }

        char    name [64];
        int     i;
        size_t  counter;
        double  d;
};

// ----------------------------------------------------------------------------

static bool operator < (const data_class &lhs, const data_class &rhs)  {

    return (lhs.counter < rhs.counter);
}

// ----------------------------------------------------------------------------

// typedef DMSob_ObjectBase<header_class,
//                          data_class,
//                          DMScu_MMapFile>    MyObjBase;
typedef DMSob_ObjectBase<header_class,
                         data_class,
                         DMScu_MMapSharedMem>   MyObjBase;

static  const   size_t  ITER_COUNT = 10000;
static  const   char    *OBJECT_BASE_NAME = "/testfile";
// static  const   char    *OBJECT_BASE_NAME = "/tmp/testfile";

// ----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

    cout.precision (5);

    header_class    my_header;

    strcpy (my_header.name, "This is a Header");
    my_header.i = -10000;

    MyObjBase   write_objbase (OBJECT_BASE_NAME, MyObjBase::_write_,
                               MyObjBase::_random_, my_header);

    srand (5);
    srand48 (5);

    write_objbase.dettach ();
    write_objbase.attach ();

    // write_objbase.reserve (ITER_COUNT * 2);
    for (size_t i = 0; i < ITER_COUNT; ++i)  {
        const   int               the_int = rand ();
        const   double            the_double = drand48 ();
        DMScu_FixedSizeString<63> str;

        str.printf ("the number is %d", the_int);

        data_class  dc;

        strcpy (dc.name, str.c_str ());
        dc.i = the_int;
        dc.counter = i;
        dc.d = the_double;

        if (i % 2)
            write_objbase << dc;
        else
            write_objbase.push_back (dc);
        // cout << "Inserted record number " << i << endl;
    }
    cout << "Inserted total of " << write_objbase.size ()
         << " records" << endl;
    cout << endl;
    write_objbase.flush ();
    write_objbase.set_access_mode (MyObjBase::_random_);

    write_objbase.dettach ();
    write_objbase.attach ();

    const   MyObjBase   read_objbase (OBJECT_BASE_NAME, MyObjBase::_read_,
                                      MyObjBase::_random_, my_header);

    srand (5);
    srand48 (5);

    cout << "\nTesting the print function:\n";
    read_objbase.back ().print ();
    cout << "Print function was successfull\n" << endl;

    if (read_objbase.object_count () != write_objbase.object_count ())  {
        cout << "Object count of write file: "
             << write_objbase.object_count ()
             << " doesn't match the object count of read file: "
             << read_objbase.object_count () << endl;
        return (EXIT_FAILURE);
    }

    for (size_t i = 0; i < ITER_COUNT; ++i)  {
        const   int               the_int = rand ();
        const   double            the_double = drand48 ();
        DMScu_FixedSizeString<63> str;

        str.printf ("the number is %d", the_int);

        data_class  dc;
        data_class  read_dc;

        strcpy (dc.name, str.c_str ());
        dc.i = the_int;
        dc.counter = i;
        dc.d = the_double;

        read_objbase >> read_dc;
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

    const   MyObjBase   c_read_objbase (OBJECT_BASE_NAME, MyObjBase::_read_,
                                        MyObjBase::_random_, my_header);

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
         << write_objbase.object_count () << endl;
    cout << "Object count of read_objbase: "
         << read_objbase.object_count () << endl;
    cout << "Object count of c_read_objbase: "
         << c_read_objbase.object_count () << endl;
    cout << endl;

    time_t  ct = 0;

    ct = write_objbase.creation_time ();
    cout << "Creation_time of write_objbase: " << ctime (&ct);
    ct = read_objbase.creation_time ();
    cout << "Creation_time of read_objbase: " << ctime (&ct);
    ct = c_read_objbase.creation_time ();
    cout << "Creation_time of c_read_objbase: " << ctime (&ct);
    cout << endl << endl;

    header_class            &tmp_header = write_objbase.get_header_rec ();
    const   header_class    &c_tmp_header = c_read_objbase.get_header_rec ();

    if (! (my_header == tmp_header) && (my_header == c_tmp_header))  {
        cout << "Some thing wrong with reading the header:\n"
             << "\tmy_header.name = '" << my_header.name  << "',  "
             << "my_header.i = '" << my_header.i << "'\n"
             << "\ttmp_header.name = '" << tmp_header.name  << "',  "
             << "tmp_header.i = '" << tmp_header.i << "'\n"
             << "\tc_tmp_header.name = '" << c_tmp_header.name  << "',  "
             << "c_tmp_header.i = '" << c_tmp_header.i << "'\n"
             << endl;

            return (EXIT_FAILURE);
    }
    cout << "Comparison of header records was successfull." << endl;

    strcpy (tmp_header.name, "This is a change");
    if (tmp_header == c_tmp_header && (! (my_header == c_tmp_header)))  {
        cout << "Comparison of header records after the change "
                "was successfull.\n" << endl;
    }
    else  {
        cout << "\nSome thing wrong with changing the header:\n" << endl;
    }

    cout << "Current offsets:\n"
         << "write_objbase : " << write_objbase.tell () << "\n"
         << "read_objbase : " << read_objbase.tell () << "\n"
         << "c_read_objbase : " << c_read_objbase.tell () << "\n"
         << endl;

    cout << "\nTesting the forward iterators:\n\n";

    for (MyObjBase::const_iterator citr = write_objbase.begin ();
         citr != write_objbase.end (); ++citr)  {
        cout << "\t" << citr->counter << endl;
    }

    cout << "\nTesting the reverse iterators:\n\n";

    for (MyObjBase::const_reverse_iterator critr = write_objbase.rbegin ();
         critr != write_objbase.rend (); ++critr)  {
        cout << "\t" << critr->counter << endl;
    }

   // Just checking if they compile
   //
    const   MyObjBase::iterator                 itr = write_objbase.begin ();
    const   MyObjBase::const_iterator           citr = itr;
    const   MyObjBase::const_reverse_iterator   critr = citr;
    const   MyObjBase::reverse_iterator         ritr = itr;

    write_objbase.dettach ();
    write_objbase.attach ();

    DMSobRT_DatabaseOrganizer<data_class,
                              std::less<data_class>,
                              header_class> ts_org ("20070930",
                                                    "/tmp/xxxx",
                                                    "/tmp/yyyy",
                                                    20070930);

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
