// Hossein Moein
// September 23, 2007

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

#include <time.h>
#include <strings.h>

#include <DMScu_FixedSizeString.h>
#include <DMSob_FileObject.h>

using namespace std;

//-----------------------------------------------------------------------------

class  header_class  {

    public:

        inline header_class () : i (0)  { name [0] = 0; unused [0] = 0;}
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

//-----------------------------------------------------------------------------

class  data_class  {

    public:

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

//-----------------------------------------------------------------------------

typedef DMSob_FileObject<header_class, data_class>  MyFileObj;

static  const   size_t  ITER_COUNT = 10000;
static  const   char    *FILE_OBJECT_NAME = "testfile";

//-----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

    cout.precision (5);

    header_class    my_header;

    strcpy (my_header.name, "This is a Header");
    my_header.i = -10000;

    MyFileObj   write_fileobj (FILE_OBJECT_NAME, MyFileObj::_write_,
                               MyFileObj::_random_, my_header);

    srand (5);
    srand48 (5);

    write_fileobj.dettach ();
    write_fileobj.attach ();

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

        write_fileobj << dc;
        // cout << "Inserted record number " << i << endl;
    }
    cout << "Inserted total of " << ITER_COUNT << " records" << endl;
    cout << endl;
    write_fileobj.flush ();
    write_fileobj.set_access_mode (MyFileObj::_random_);

    write_fileobj.dettach ();
    write_fileobj.attach ();

    MyFileObj   read_fileobj (FILE_OBJECT_NAME, MyFileObj::_read_,
                              MyFileObj::_random_, my_header);

    srand (5);
    srand48 (5);

    cout << "\nTesting the print function:\n";
    read_fileobj [2].print ();
    cout << "Print function was successfull\n" << endl;

    if (read_fileobj.object_count () != write_fileobj.object_count ())  {
        cout << "Object count of write file: "
             << write_fileobj.object_count ()
             << " doesn't match the object count of read file: "
             << read_fileobj.object_count () << endl;
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

        read_fileobj >> read_dc;
        if (! (dc == read_dc))  {
            cout << "Something doesn't add up:\n"
                 << "\tCounter = '" << i << "'\n"
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

    if (! (read_fileobj [2] == write_fileobj [2]))  {
        cout << "Something wrong with the call to [] operator:\n"
             << "\tread_fileobj [2].name = '"
             << read_fileobj [2].name << "',  "
             << "read_fileobj [2].i = '" << read_fileobj [2].i << "',  "
             << "read_fileobj [2].counter = '"
             << read_fileobj [2].counter << "',  "
             << "read_fileobj [2].d = '" << read_fileobj [2].d << "'\n"
             << "\twrite_fileobj [2].name = '"
             << write_fileobj [2].name << "',  "
             << "write_fileobj [2].i = '" << write_fileobj [2].i << "',  "
             << "write_fileobj [2].counter = '"
             << write_fileobj [2].counter << "',  "
             << "write_fileobj [2].d = '" << write_fileobj [2].d << "'\n"
             << endl;

            return (EXIT_FAILURE);
    }
    cout << "Compared read_fileobj [2] == write_fileobj [2]" << endl;

    MyFileObj   c_read_fileobj (FILE_OBJECT_NAME, MyFileObj::_read_,
                                MyFileObj::_random_, my_header);

    if (! (c_read_fileobj [3] == write_fileobj [3]))  {
        cout << "Something wrong with the call to [] operator:\n"
             << "\tc_read_fileobj [2].name = '"
             << c_read_fileobj [2].name << "',  "
             << "c_read_fileobj [2].i = '" << c_read_fileobj [2].i << "',  "
             << "c_read_fileobj [2].counter = '"
             << c_read_fileobj [2].counter << "',  "
             << "c_read_fileobj [2].d = '" << c_read_fileobj [2].d << "'\n"
             << "\twrite_fileobj [2].name = '"
             << write_fileobj [2].name << "',  "
             << "write_fileobj [2].i = '" << write_fileobj [2].i << "',  "
             << "write_fileobj [2].counter = '"
             << write_fileobj [2].counter << "',  "
             << "write_fileobj [2].d = '" << write_fileobj [2].d << "'\n"
             << endl;

            return (EXIT_FAILURE);
    }
    cout << "Compared c_read_fileobj [2] == write_fileobj [2]" << endl;
    cout << endl;

    cout << "Object count of write_fileobj: "
         << write_fileobj.object_count () << endl;
    cout << "Object count of read_fileobj: "
         << read_fileobj.object_count () << endl;
    cout << "Object count of c_read_fileobj: "
         << c_read_fileobj.object_count () << endl;
    cout << endl;

    time_t  ct = 0;

    ct = write_fileobj.creation_time ();
    cout << "Creation_time of write_fileobj: " << ctime (&ct);
    ct = read_fileobj.creation_time ();
    cout << "Creation_time of read_fileobj: " << ctime (&ct);
    ct = c_read_fileobj.creation_time ();
    cout << "Creation_time of c_read_fileobj: " << ctime (&ct);
    cout << endl << endl;

    header_class            tmp_header = write_fileobj.get_header_rec ();
    const   header_class    c_tmp_header = c_read_fileobj.get_header_rec ();

    if (! (my_header == tmp_header) && (my_header == c_tmp_header))  {
        cout << "Something wrong with reading the header:\n"
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

    if (tmp_header == c_tmp_header && my_header == c_tmp_header)  {
        cout << "Comparison of header records after the change "
                "was successfull.\n" << endl;
    }
    else  {
        cout << "\nSomething wrong with changing the header:\n" << endl;
        return (EXIT_FAILURE);
    }

    cout << "Current offsets:\n"
         << "write_fileobj : " << write_fileobj.tell () << "\n"
         << "read_fileobj : " << read_fileobj.tell () << "\n"
         << "c_read_fileobj : " << c_read_fileobj.tell () << "\n"
         << endl;

    write_fileobj.dettach ();
    write_fileobj.attach ();

    return (EXIT_SUCCESS);
}

//-----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
