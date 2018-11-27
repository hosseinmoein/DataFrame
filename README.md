[![Build status](https://ci.appveyor.com/api/projects/status/hjw01qui3bvxs8yi?svg=true)](https://ci.appveyor.com/project/justinjk007/dataframe)
[![Build Status](https://travis-ci.org/justinjk007/DataFrame.svg?branch=master)](https://travis-ci.org/justinjk007/DataFrame)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard )

![Alt text](docs/pandalion.png "C++ protecting Python")

# DataFrame
This is a C++ statistical library that provides an interface similar to Pandas package in Python.<BR>
<B>A DataFrame can have one index column and many data columns of any built-in or user-defined type</B>.<BR>
You could do almost everything you could do with Pandas within the C++ syntax and type checking framework. You can add/delete any data column type, slice, run summarization functors, transpose, etc. like Pandas.<BR><BR>
<B>Views</B><BR>
You can slice the data frame and instead of getting another data frame you can opt to get a view. A view is a data frame that is a reference to a slice of the original data frame. So if you change the data in the view the corresponding data in the original data frame will also be changed.<BR><BR>
<B>Multithreading</B><BR>
Instances of DataFrame are not multithreaded safe. In other words, instances of DataFrame must not be used in multiple threads without protection. But DataFrame utilizes multithreading in two different ways:<BR>
    1) There are asynchronous versions of some methods. For example, you have both sort() and sort_async(). The later returns a std::future which could execute in parallel.<BR>
    2) DataFrame uses multiple threads, internally and unbeknown to the user, in some of its algorithms when appropriate. User can control (or turn off) the multithreading by calling set_thread_level() which sets the max number of threads to be used. The default is 0. The optimal number of threads is a function of users hardware/software environment and usually obtained by trail and error.<BR>



## [Documentation](docs/DataFrameDoc.pdf)

## [DataFrame Test File](src/dataframe_tester.cc)
[Heterogeneous Vectors Test File](src/vectors_tester.cc)

## [Contributions](docs/CONTRIBUTING.md)

## [License](License)


### Installing using CMake
```
mkdir build
cd build
cmake ..
make install
```

### Uninstalling

```
cd build
make uninstall
```
