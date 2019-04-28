// Hossein Moein
// July 17 2009

#include <cstdlib>
#include <iostream>

#include <DMScu_PtrVector.h>

using namespace std;

//-----------------------------------------------------------------------------

class   AbsBase  {

    public:

        AbsBase (int i) : _value (i)  {   }

        virtual void print () = 0;

    protected:

        int _value;
};

//-----------------------------------------------------------------------------

class   Derv : public AbsBase  {

    public:

        Derv (int i) : AbsBase (i)  {   }

        virtual void print ()  { cout << "The value is: " << _value << endl; }
};

//-----------------------------------------------------------------------------

typedef DMScu_PtrVector<int>        int_vec;
typedef DMScu_PtrVector<AbsBase>    abs_vec;

void dump (const int_vec &vec);

//-----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

    int_vec vec;
    int     *iptr = NULL;

    vec.push_back (new int (1));
    vec.push_back (new int (2));
    vec.push_back (new int (3));
    vec.push_back (new int (4));
    vec.push_back (new int (5));
    vec.push_back (new int (6));
    vec.push_back (new int (7));
    vec.push_back (new int (8));
    vec.push_back (new int (9));
    vec.push_back (new int (10));
    vec.push_back (new int (11));
    vec.push_back (new int (12));
    vec.push_back (new int (13));
    vec.push_back (new int (14));
    vec.push_back (new int (15));
    vec.push_back (new int (16));
    vec.push_back (new int (17));
    vec.push_back (new int (18));
    vec.push_back (iptr = new int (19));
    vec.push_back (new int (20));
    dump (vec);

    abs_vec avec;

    avec.push_back (new Derv (1));
    avec.push_back (new Derv (2));
    avec.push_back (new Derv (3));
    avec.push_back (new Derv (4));
    avec.push_back (new Derv (5));
    avec.push_back (new Derv (6));
    avec.push_back (new Derv (7));
    avec.push_back (new Derv (8));
    avec.push_back (new Derv (9));
    avec.push_back (new Derv (10));
    avec.push_back (new Derv (11));
    avec.push_back (new Derv (12));
    avec.push_back (new Derv (13));
    avec.push_back (new Derv (14));
    avec.push_back (new Derv (15));
    avec.push_back (new Derv (16));
    avec.push_back (new Derv (17));
    avec.push_back (new Derv (18));

    cout << endl << "--- After erasing 3 ..." << endl << endl;
    vec.erase (vec.begin() + 2);
    dump (vec);

    cout << endl << "--- After = operator ..." << endl << endl;

    int_vec vec2;

    vec2 = vec;
    dump (vec2);

    cout << endl
         << "--- After re-inserting 3 and erasing 10 through 14 "
            "(inclusive)..."
         << endl << endl;

    vec2.insert (vec2.begin () + 2, new int (3));
    vec2.erase (vec2.begin () + 9, vec2.begin () + 14);
    dump (vec2);

    int_vec::const_iterator citr = vec2.find (3);

    if (citr != vec2.end ())  {
        cout << "SUCCESS: vec2 find " << **citr << endl;
    }

    cout << endl << "--- After erasing 19 through a pointer ..."
         << endl << endl;

    vec.erase (iptr);
    dump (vec);

    cout << endl << "--- Testing the resize() method ..."
         << endl << endl;
    vec.resize (30);
    dump (vec);
    vec.resize (10);
    dump (vec);

    vec.clear ();

    cout << endl << "--- Testing the Abstract class ..." << endl << endl;

    for (abs_vec::const_iterator itr = avec.begin ();
         itr != avec.end (); ++itr)  {
        (*itr)->print ();
    }

    cout << endl;

    return (EXIT_SUCCESS);
}

//-----------------------------------------------------------------------------

void dump (const int_vec &vec)  {

    for (int_vec::const_iterator itr = vec.begin ();
         itr != vec.end (); ++itr)  {
        if (*itr == NULL)
            cout << "   " << "--NULL--";
        else
            cout << "   " << **itr;
    }

    cout << "\n===" << endl;

    for (int_vec::const_reverse_iterator itr = vec.rbegin ();
         itr != vec.rend(); ++itr)  {
        if (*itr == NULL)
            cout << "   " << "--NULL--";
        else
            cout << "   " << **itr;
    }

    cout << endl;
}

//-----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
