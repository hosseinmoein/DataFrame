#include <DataFrame/MMap/ObjectVector.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <time.h>

using namespace std;
using namespace hmdf;

// ----------------------------------------------------------------------------

class  data_class  {

    public:

        typedef unsigned int    size_type;
        typedef time_t          time_type;

        inline data_class () : counter (0)  { name [0] = 0; }
        inline data_class (const data_class &that)  { *this = that; }

        inline data_class &operator = (const data_class &rhs)  {

            if (this != &rhs)  {
                strcpy (name, rhs.name);
                counter = rhs.counter;
            }

            return (*this);
        }

        bool operator == (const data_class &rhs) const  {

            return (! strcmp (name, rhs.name) && counter == rhs.counter);
        }

        void print () const  { cout << "\tcounter = " << counter << endl; }

        char    name [64];
        int     counter;
};

// ----------------------------------------------------------------------------

static bool operator < (const data_class &lhs, const data_class &rhs)  {

    return (lhs.counter < rhs.counter);
}

// ----------------------------------------------------------------------------

#ifndef _WIN32

typedef MMapVector<data_class>  MyObjBase;

static const size_t ITER_COUNT = 1000;
static const char   *OBJECT_BASE_NAME = "./testfile";

#endif // _WIN32

// ----------------------------------------------------------------------------

int main (int argc, char *argv [])  {

#ifndef _WIN32

    cout.precision (20);

    MyObjBase   write_objbase (OBJECT_BASE_NAME);

    write_objbase.reserve (ITER_COUNT * 2);

    for (size_t i = 0; i < ITER_COUNT; ++i)  {
        FixedSizeString<63> str;

        str.printf ("the number is %d", i);

        data_class  dc;

        strcpy (dc.name, str.c_str ());
        dc.counter = i;

        write_objbase.push_back (dc);
        if (i % 1000 == 0)
            cout << "Inserted record number " << i << endl;
    }
    cout << "Inserted total of " << write_objbase.size ()
         << " records\n" << endl;

    MyObjBase   appd_object (OBJECT_BASE_NAME);

    cout << "Erasing 100 - 900\n" << endl;

    const MyObjBase::iterator   eiter =
        appd_object.erase (appd_object.begin() + 100,
                           appd_object.begin() + 900 + 1);

    cout << "After erasing, we have " << appd_object.size ()
         << " objects left" << endl;
    if (eiter == appd_object.end ())
        cout << "Return iterator is the end iterator" << endl;
    else
        cout << "Return iterator is " << eiter->counter << endl;

    size_t  counter = 0;
	
    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        if (++counter % 100 == 0)
            citer->print ();
    cout << endl;

    cout << "Erasing 5\n" << endl;

    const MyObjBase::iterator   eiter2 =
        appd_object.erase (appd_object.begin() + 5);

    cout << "After erasing, we have " << appd_object.size ()
         << " objects left" << endl;
    if (eiter2 == appd_object.end ())
        cout << "Return iterator is the end iterator" << endl;
    else
        cout << "Return iterator is " << eiter2->counter << endl;

    counter = 0;
    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        if (++counter % 100 == 0)
            citer->print ();
    cout << endl;

    cout << "Inserting 5\n" << endl;

    data_class  dc;

    dc.counter = 5;
    appd_object.insert (appd_object.begin() + 5, dc);

    counter = 0;
    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        if (++counter % 100 == 0)
            citer->print ();
    cout << endl;

    cout << "Inserting -1\n" << endl;

    dc.counter = -1;
    appd_object.insert (appd_object.begin(), dc);

    counter = 0;
    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        if (++counter % 100 == 0)
            citer->print ();
    cout << endl;

    cout << "Inserting 1000\n" << endl;

    dc.counter = 1000;
    appd_object.insert (appd_object.end(), dc);

    counter = 0;
    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        if (++counter % 100 == 0)
            citer->print ();
    cout << endl;

#endif // _WIN32

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
