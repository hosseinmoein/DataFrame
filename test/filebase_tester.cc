// Hossein Moein
// May 29, 2019
// Copyright (C) 2018-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/MMap/FileBase.h>

#ifndef _WIN32

using namespace hmdf;

bool WriteFile (FileBase &);
bool ReadFile (FileBase &);

#endif // _WIN32

//-----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

#ifndef _WIN32

    // FileBase    fbase_file ("test.fbase", FileBase::_bwrite_, 10000);
    FileBase    fbase_file ("test.fbase", FileBase::_write_, 10000);

    if (! fbase_file.is_ok ())  {
        printf ("Unable to create the fbase file\n");
        return (-1);
    }

    if (! WriteFile (fbase_file))  {
        printf ("Error in writing\n");
        return (-1);
    }

    fbase_file.close ();
    // FileBase    read_file ("test.fbase", FileBase::_bread_);
    FileBase    read_file ("test.fbase", FileBase::_read_);

    read_file.close ();
    read_file.open ();

    if (! read_file.is_ok ())  {
        printf ("Unable to open file for reading\n");
        return (-1);
    }

    if (! ReadFile (read_file))  {
        printf ("Error in writing\n");
        return (-1);
    }

    read_file.close ();
    read_file.open ();

    FileBase    write_file ("test.fbase", FileBase::_append_, 10000);

    write_file.close ();
    write_file.open ();
    write_file.close ();
    write_file.open ();

    FileBase    read_file2 ("test.fbase", FileBase::_read_);

    read_file2.close ();
    read_file2.open ();

    char    buffer [2048];

    read_file2.go_to_line (3);
    read_file2.get_line (buffer);
    std::cout << "Line 3  is:  " << buffer << std::endl;
    write_file.go_to_line (8);
    write_file.get_line (buffer);
    std::cout << "Line 8  is:  " << buffer << std::endl;
    write_file.go_to_line (0);
    write_file.get_line (buffer);
    std::cout << "Line 0  is:  " << buffer << std::endl;
    read_file2.go_to_line (10);
    read_file2.get_line (buffer);
    std::cout << "Line 10 is:  " << buffer << std::endl;
    read_file2.go_to_line (5);
    read_file2.get_line (buffer);
    std::cout << "Line 5  is:  " << buffer << std::endl;
    read_file2.go_to_line (0);
    read_file2.get_line (buffer);
    std::cout << "Line 0  is:  " << buffer << std::endl;
    read_file2.go_to_line (10);
    read_file2.get_line (buffer);
    std::cout << "Line 10 is:  " << buffer << std::endl;

   // Throw an exception
   //
    try  {
        read_file2.go_to_line (1000);
        read_file2.get_line (buffer);
        std::cout << "Line 10000 is:  " << buffer << std::endl;
    }
    catch (std::exception &ex)  {
        std::cerr << "Expected EXCEPTION: " << ex.what () << std::endl;
    }

    read_file2.go_to_line (3);
    read_file2.get_line (buffer);
    std::cout << "Line 3  is:  " << buffer << std::endl;
    read_file2.go_to_line (8);
    read_file2.get_line (buffer);
    std::cout << "Line 8  is:  " << buffer << std::endl;
    read_file2.go_to_line (0);
    read_file2.get_line (buffer);
    std::cout << "Line 0  is:  " << buffer << std::endl;
    read_file2.go_to_line (10);
    read_file2.get_line (buffer);
    std::cout << "Line 10 is:  " << buffer << std::endl;
    read_file2.go_to_line (5);
    read_file2.get_line (buffer);
    std::cout << "Line 5  is:  " << buffer << std::endl;
    read_file2.go_to_line (0);
    read_file2.get_line (buffer);
    std::cout << "Line 0  is:  " << buffer << std::endl;
    read_file2.go_to_line (10);
    read_file2.get_line (buffer);
    std::cout << "Line 10 is:  " << buffer << std::endl;

    read_file2.close ();
    write_file.unlink ();

    FileBase    c_file ("./sample_data_string_index.csv",
                        FileBase::_read_,
                        10000);
    char        line [1024];

    while (! c_file.is_eof ())  {
        c_file.get_line (line);
        std::cout << line << std::endl;
    }

#endif // _WIN32

    return (0);
}

//-----------------------------------------------------------------------------

#ifndef _WIN32

bool WriteFile (FileBase & fbase_file)  {

    const double    dvar = 1.67890128976584;

    fbase_file.set_precision (3);
    for (int index = 0; index < 1000; ++index)  {
        fbase_file << dvar + index << '\n';
    }

    return (true);
}

//-----------------------------------------------------------------------------

bool ReadFile (FileBase &fbase_file)  {

    double  var = 0.0;

    fbase_file.set_precision (3);
    while (! fbase_file.is_eof ())  {
        try  {
            fbase_file >> var;
            fbase_file.get_char (); // Read the linefeed
        }
        catch (std::exception &ex)  {
            std::cerr << "EXCEPTION: " << ex.what () << std::endl;
            return -1;
        }
        std::cout << var << std::endl;
    }

  // Just to see it throwing an exception
  //
    try  {
        fbase_file >> var;
    }
    catch (std::exception &ex)  {
        std::cerr << "Expected EXCEPTION: " << ex.what () << std::endl;
        return (true);
    }

    return (true);
}

#endif // _WIN32

//-----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
