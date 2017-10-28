// Hossein Moein
// August 21, 2007

#include <fstream>

#include <DMScu_MMapFile.h>

int WriteFile (DMScu_MMapFile &);
int ReadFile (DMScu_MMapFile &);

//-----------------------------------------------------------------------------

int main (int argCnt, char *argVctr [])  {

//    DMScu_MMapFile  mmap_file ("test.mmap", DMScu_MMapFile::_bwrite_, 10000);
    DMScu_MMapFile  mmap_file ("test.mmap", DMScu_MMapFile::_write_, 10000);

    if (! mmap_file.is_ok ())  {
        printf ("Unable to create the mmap file\n");
        return EXIT_FAILURE;
    }

    if (WriteFile (mmap_file) < 0)  {
        printf ("Error in writing\n");
        return EXIT_FAILURE;
    }

    mmap_file.close ();
//     DMScu_MMapFile  read_file ("test.mmap", DMScu_MMapFile::_bread_);
    DMScu_MMapFile  read_file ("test.mmap", DMScu_MMapFile::_read_);

    read_file.close ();
    read_file.open ();

    if (! read_file.is_ok ())  {
        printf ("Unable to open file for reading\n");
        return EXIT_FAILURE;
    }

    if (ReadFile (read_file) < 0)  {
        printf ("Error in writing\n");
        return EXIT_FAILURE;
    }

    read_file.close ();
    read_file.open ();

    DMScu_MMapFile  write_file ("test.mmap", DMScu_MMapFile::_append_, 10000);

    std::ifstream    cifs ("/export/home/moeinh/work/HITS/src/DMScu/"
                           "src/DMScu_MMapBase.cc");
    std::ifstream    hifs ("/export/home/moeinh/work/HITS/src/DMScu/"
                           "include/DMScu_MMapBase.h");

    write_file.close ();
    write_file.open ();
    write_file.close ();
    write_file.open ();

    write_file << cifs;
    write_file << hifs;

    cifs.close ();
    hifs.close ();
    write_file.close ();

    DMScu_MMapFile  read_file2 ("test.mmap", DMScu_MMapFile::_read_);

    read_file2.close ();
    read_file2.open ();

    char    buffer [2048];

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

   // Throw an exception
   //
    try  {
        read_file2.go_to_line (1000);
        read_file2.get_line (buffer);
        std::cout << "Line 10000 is:  " << buffer << std::endl;
    }
    catch (DMScu_Exception &ex)  {
        std::cerr << "EXCEPTION: " << ex.what () << std::endl;
    }

    std::cout << "Testing remap() ..." << std::endl;

    read_file2.remap (10000);
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

    std::cout << "System Page Size: " << DMScu_MMapBase::SYSTEM_PAGE_SIZE
              << std::endl;

    read_file2.close ();
    write_file.unlink ();
    return EXIT_SUCCESS;
}

//-----------------------------------------------------------------------------

int WriteFile (DMScu_MMapFile & mmapFile)  {

    const   double  dvar = 1.67890128976584;
//    const   double  dvar = 1.6;

    mmapFile.set_precision (3);
    for (int index = 0; index < 1000; ++index)
//        mmapFile << dvar + index;
        mmapFile << dvar + index << '\n';

    return 0;
}

//-----------------------------------------------------------------------------

int ReadFile (DMScu_MMapFile &mmapFile)  {

    double  var = 0.0;

    mmapFile.set_precision (3);
    while (! mmapFile.is_eof ())  {
        try  {
            mmapFile >> var;
        }
        catch (DMScu_Exception &ex)  {
            std::cerr << "EXCEPTION: " << ex.what () << std::endl;
            return -1;
        }
        std::cout << var << std::endl;
    }

  // Just to see it throwing an exception
  //
    // std::cerr << "This exception is supposed to happen:" << std::endl;
    // try  {
    //     mmapFile >> var;
    // }
    // catch (DMScu_Exception &ex)  {
    //     std::cerr << "EXCEPTION: " << ex.what () << std::endl;
    //     return 0;
    // }

    return 0;
}

//-----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
