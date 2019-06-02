// Hossein Moein
// August 13, 2008

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>

#include <time.h>
#include <strings.h>

#include <DMScu_FixedSizeString.h>

#include <DMSob_ObjectBase.h>

using namespace std;

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

typedef DMSob_ObjectBase<time_t, data_class, DMScu_MMapFile>    MyObjBase;

static  const   size_t  ITER_COUNT = 1000;
static  const   char    *OBJECT_BASE_NAME = "/tmp/testfile";

// ----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

    cout.precision (20);

    time_t  t = 555444;

    MyObjBase   write_objbase (OBJECT_BASE_NAME, MyObjBase::_write_,
                               MyObjBase::_random_, t);

    write_objbase.reserve (ITER_COUNT * 2);

    for (size_t i = 0; i < ITER_COUNT; ++i)  {
        DMScu_FixedSizeString<63> str;

        str.printf ("the number is %d", i);

        data_class  dc;

        strcpy (dc.name, str.c_str ());
        dc.counter = i;
            write_objbase.push_back (dc);
    }
    cout << "Inserted total of " << write_objbase.size ()
         << " records\n" << endl;

    write_objbase.dettach ();

    MyObjBase   appd_object (OBJECT_BASE_NAME, MyObjBase::_append_,
                             MyObjBase::_random_, t);

    cout << "Erasing 100 - 900\n" << endl;

    const   MyObjBase::iterator eiter =
        appd_object.erase (appd_object.iterator_at (100),
                           appd_object.iterator_at (900 + 1));

    cout << "After erasing, we have " << appd_object.size ()
         << " objects left" << endl;
    if (eiter == appd_object.end ())
        cout << "Return iterator is the end iterator" << endl;
    else
        cout << "Return iterator is " << eiter->counter << endl;

    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        citer->print ();
    cout << endl;

    cout << "Erasing 5\n" << endl;

    const   MyObjBase::iterator eiter2 =
        appd_object.erase (appd_object.iterator_at (5));

    cout << "After erasing, we have " << appd_object.size ()
         << " objects left" << endl;
    if (eiter2 == appd_object.end ())
        cout << "Return iterator is the end iterator" << endl;
    else
        cout << "Return iterator is " << eiter2->counter << endl;

    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        citer->print ();
    cout << endl;

    cout << "Inserting 5\n" << endl;

    data_class  dc;

    dc.counter = 5;
    appd_object.insert (appd_object.iterator_at (5), dc);

    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        citer->print ();
    cout << endl;

    cout << "Inserting -1\n" << endl;

    dc.counter = -1;
    appd_object.insert (appd_object.begin (), dc);

    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        citer->print ();
    cout << endl;

    cout << "Inserting 1000\n" << endl;

    dc.counter = 1000;
    appd_object.insert (appd_object.end (), dc);

    for (MyObjBase::const_iterator citer = appd_object.begin ();
         citer != appd_object.end (); ++citer)
        citer->print ();
    cout << endl;

    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
