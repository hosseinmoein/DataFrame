// Hossein Moein
// July 17 2009

#include <cstdlib>
#include <string.h>
#include <time.h>
#include <iostream>
#include <string>

#include "../include/DMScu_FixedSizeString.h"

using namespace std;

// ----------------------------------------------------------------------------

int main (int arg_cnt, char *arg_vctr [])  {

    const   size_t                  the_size = 32;
    DMScu_FixedSizeString<the_size> the_str;
    DMScu_FixedSizeString<the_size> the_str2 = the_str;
    DMScu_FixedSizeString<the_size> the_str3 (the_str);

    cout << "The size is: " << the_size << endl;
    cout << "capacity(): " << the_str.capacity () << endl;
    cout << "size(): " << the_str.size () << endl;
    cout << "c_str(): '" << the_str.c_str () << "'" << endl;
    cout << "Size of DMScu_FixedSizeString<32>: " << sizeof the_str << endl;

    DMScu_VirtualString::const_pointer  str_1 = "This is a test";

    cout << "str_1: " << str_1 << endl;

    the_str = str_1;
    cout << "After the_str = str_1;\n" << the_str.c_str () << endl;
    cout << "size(): " << the_str.size () << endl;

    DMScu_VirtualString::const_pointer  str_2 = "This string 2";

    cout << "str_2: " << str_2 << endl;

    the_str = str_2;
    cout << "After the_str = str_2;\n" << the_str.c_str () << endl;
    cout << "size(): " << the_str.size () << endl;

    the_str = str_1;
    the_str += str_2;
    cout << "After the_str = str_1; " << endl
         << "      the_str += str_2; " << endl
         << the_str.c_str ()
         << endl;
    cout << "size(): " << the_str.size () << endl;
    cout << "Result of the_str == str_1;\n" << (the_str == str_1) << endl;
    cout << "Result of the_str != str_1;\n" << (the_str != str_1) << endl;

    the_str = str_1;
    cout << "After the_str = str_1; " << endl;
    cout << "Result of the_str == str_1;\n" << (the_str == str_1) << endl;
    cout << "Result of the_str != str_1;\n" << (the_str != str_1) << endl;

    the_str.printf ("%d %lf", 12, 20.4356);
    cout << "After the_str.printf (\"%d %lf\", 12, 20.4356);\n"
         << the_str.c_str () << endl;

    cout << "The 3rd char is: " << the_str [3] << endl;
    the_str [3] = 'X';
    cout << "After the_str [3] = 'X'; The 3rd char is: "
         << the_str [3] << endl
         << the_str.c_str () << endl;

    cout << "Is the_str empty? " << the_str.empty () << endl;
    the_str.clear ();
//    the_str = "";
    cout << "After clear(), is the_str empty? " << the_str.empty () << endl;

    DMScu_FixedSizeString<the_size> the_str4 ("String passed to constructor");

    cout << "After the_str4 (\"String passed to constructor\");\n"
         << the_str4.c_str () << endl;

    DMScu_FixedSizeString<28>   str28 = "This is a 28 char string";
    DMScu_FixedSizeString<64>   str64 = "This is a 64 char string. "
                                 "I am going to make it longer than 28 chars.";
    DMScu_VirtualString         &vstr28 = str28;
    DMScu_VirtualString         &vstr64 = str64;

    cout << "\n\n-- Testing the abstract base class\n\n";
    {
        cout << "vstr28 = '" << vstr28.c_str () << "'" << endl;
        cout << "vstr64 = '" << vstr64.c_str () << "'" << endl;
    }

    cout << "\n\n-- Testing the iterators on vstr64\n\n";
    {
        for (DMScu_VirtualString::const_iterator itr = str64.begin ();
             itr != vstr64.end (); ++itr)
            cout << "  '" << *itr << "'";
        cout << endl;
    }

    cout << "\n\n-- Testing the comparison operators\n\n";
    {
        DMScu_FixedSizeString<28>   str1 = "AAAAx";
        DMScu_FixedSizeString<18>   str2 = "AAAA";

        cout << "AAAAx > AAAA -> " << (str1 > str2) << endl;
        str1 = "Azzz";
        str2 = "Bxxx";
        cout << "Azzz > Bxxx -> " << (str1 > str2) << endl;
        str1 = "AAAA";
        str2 = "AAAA";
        cout << "AAAA > AAAA -> " << (str1 > str2) << endl;
        str1 = "AAAz";
        str2 = "AAAx";
        cout << "AAAz > AAAx -> " << (str1 > str2) << endl;
    }

    cout << "\n\n-- Testing the find methods\n\n";
    {
        DMScu_FixedSizeString<28>   str = "A.B.CDEFGHIJKLMN";

        if (str.find ('.') != 1)  {
            cout << "ERROR: str.find ('.') failed" << endl;
            return (-1);
        }
        if (str.find ('.', 2) != 3)  {
            cout << "ERROR: str.find ('.', 2) failed" << endl;
            return (-1);
        }
        if (str.find ('.', 4) != DMScu_VirtualString::npos)  {
            cout << "ERROR: str.find ('.', 4) failed" << endl;
            return (-1);
        }
        if (str.find ('X') != DMScu_VirtualString::npos)  {
            cout << "ERROR: str.find ('X') failed" << endl;
            return (-1);
        }
        if (str.find ('.', 16) != DMScu_VirtualString::npos)  {
            cout << "ERROR: str.find ('.', 16) failed" << endl;
            return (-1);
        }

        if (str.find ("HIJ", 4) != 9)  {
            cout << "ERROR: str.find (\"HIJ\", 9) failed" << endl;
            return (-1);
        }
        if (str.find ("A.B.CD") != 0)  {
            cout << "ERROR: str.find (\"A.B.CD\") failed" << endl;
            return (-1);
        }
        if (str.find ("LMN", 13) != 13)  {
            cout << "ERROR: str.find (\"LMN\", 13) failed" << endl;
            return (-1);
        }
        if (str.find ("LMN") != 13)  {
            cout << "ERROR: str.find (\"LMN\") failed" << endl;
            return (-1);
        }
        if (str.find ("XYZ") != DMScu_VirtualString::npos)  {
            cout << "ERROR: str.find (\"XYZ\", 9) failed" << endl;
            return (-1);
        }

        cout << "SUCCESS: find method is working" << endl;
    }

    cout << "\n\n-- Testing the ncopy()\n\n";
    {
        DMScu_FixedSizeString<8>   str;

        str.ncopy ("123456", 8);
        cout << "It must be '123456' -- '" << str.c_str ()
              << "'" << endl;

        str.ncopy ("123456789012", 5);
        cout << "It must be '12345' -- '" << str.c_str ()
              << "'" << endl;

    }

    cout << "\n\n-- Testing the append_printf()\n\n";
    {
        DMScu_FixedSizeString<1023> str;

        str = "This is a string: ";
        str.append_printf ("%s %d -- ", "This is appended", 1);
        str.append_printf ("%s %d.\n", "This is appended again", 2);
        cout << str.c_str () << endl;

    }

   // I just want to make sure that these statements will compile.
   //
    str28.compare (vstr64);
    vstr64.compare (str28);

    cout << "\n\n-- Testing the replace()\n\n";
    {
        DMScu_FixedSizeString<15>   str = "USD/JPY";
        std::string                 stdstr = "USD/JPY";

        cout << "Original: " << str;
        str.replace (3, 1, "");
        stdstr.replace (3, 1, "");
        cout << " replace (3, 1, \"\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (3, 1, "\\");
        stdstr.replace (3, 1, "\\");
        cout << " replace (3, 1, \"\\\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (3, 3, "->>");
        stdstr.replace (3, 3, "->>");
        cout << " Rreplace (3, 3, \"->>\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (0, 1, "");
        stdstr.replace (0, 1, "");
        cout << " replace (0, 1, \"\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (0, 1, "S");
        stdstr.replace (0, 1, "S");
        cout << " replace (0, 1, \"S\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (0, 3, "->>");
        stdstr.replace (0, 3, "->>");
        cout << " replace (0, 3, \"->>\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (6, 1, "");
        stdstr.replace (6, 1, "");
        cout << " replace (6, 1, \"\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (6, 1, "P");
        stdstr.replace (6, 1, "P");
        cout << " replace (6, 1, \"P\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (6, 3, "->>");
        stdstr.replace (6, 3, "->>");
        cout << " replace (6, 3, \"->>\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (6, 1, "->>");
        stdstr.replace (6, 1, "->>");
        cout << " replace (6, 1, \"->>\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (5, 1, "->>");
        stdstr.replace (5, 1, "->>");
        cout << " replace (5, 1, \"->>\"): " << str << " " << stdstr << endl;

        str = "USD/JPY";
        stdstr = "USD/JPY";
        cout << "Original: " << str;
        str.replace (5, 3, "->>");
        stdstr.replace (5, 3, "->>");
        cout << " replace (5, 3, \"->>\"): " << str << " " << stdstr << endl;
    }

    cout << "\n\n-- Testing Performance\n\n";
    {
        static  const   char        *STRING = "The is a test string";
        static  const   char        *ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        char                        *str1 = ::strdup (STRING);
        char                        *str2 = ::strdup (STRING);
        DMScu_FixedSizeString<31>   my_str1 = STRING;
        DMScu_FixedSizeString<31>   my_str2 = STRING;
        std::string                 std_str1 = STRING;
        std::string                 std_str2 = STRING;
        int                         count = 0;
        const   time_t              start = ::time (NULL);

        for (int i = 0; i < 100000000; ++i)  {
            str1 [10] = ALPHA [::rand () % 25];
            str2 [10] = ALPHA [::rand () % 25];

            if (my_str1 == my_str2)
            // if (std_str1 == std_str2)
            // if (! ::strcmp (str1, str2))
                ++count;
        }

        const   time_t  end = ::time (NULL);
        cout << "String comparison took: "
             << end - start << " seconds." << endl;
    }

    return EXIT_SUCCESS;
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
