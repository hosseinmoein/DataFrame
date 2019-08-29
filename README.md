[![Build status](https://ci.appveyor.com/api/projects/status/hjw01qui3bvxs8yi?svg=true)](https://ci.appveyor.com/project/hosseinmoein/dataframe)
[![Build Status](https://travis-ci.org/hosseinmoein/DataFrame.svg?branch=master)](https://travis-ci.org/hosseinmoein/DataFrame)
![GitHub](https://img.shields.io/github/license/hosseinmoein/DataFrame.svg?color=red&style=popout)
![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/hosseinmoein/DataFrame.svg?color=blue&label=Official%20Release&style=popout)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard )

<img src="docs/pandalion.png" alt="drawing" width="500"/>

# DataFrame
This is a C++ statistical library that provides an interface similar to Pandas package in Python.<BR>
<B>A DataFrame can have one index column and many data columns of any built-in or user-defined type</B>.<BR>
You could do almost everything you could do with Pandas within the C++ syntax and type checking framework. You can add/delete any data column type, slice, run summarization functors, transpose, etc. like Pandas.<BR><BR>
<B>Views</B><BR>
You can slice the data frame and instead of getting another data frame you can opt to get a view. A view is a data frame that is a reference to a slice of the original data frame. So if you change the data in the view the corresponding data in the original data frame will also be changed.<BR><BR>
<B>Multithreading</B><BR>
Instances of DataFrame are not multithreaded safe. In other words, a single instance of DataFrame must not be used in multiple threads without protection. But DataFrame utilizes multithreading in two different ways:<BR>
1. There are asynchronous versions of some methods. For example, you have both sort() and sort_async(). The latter returns a std::future which could execute in parallel.
2. DataFrame uses multiple threads, internally and unbeknown to the user, in some of its algorithms when appropriate. User can control (or turn off) the multithreading by calling set_thread_level() which sets the max number of threads to be used. The default is 0. The optimal number of threads is a function of users hardware/software environment and usually obtained by trail and error. set_thread_level() and threading level in general is a static property and once set, it applies to all instances.

<B>Date Time</B><BR>
DateTime class included in this library is a very cool and handy object to manipulate date/time with nanosecond precision.

---

<B>Example Code</B>
```CPP
using namespace hmdf;

// Define a DataFrame with unsigned long index type
typedef StdDataFrame<unsigned long> MyDataFrame;

MyDataFrame                df;
std::vector<int>           intvec = { 1, 2, 3, 4, 5 };
std::vector<double>        dblvec = { 1.2345, 2.2345, 3.2345, 4.2345, 5.2345 };
std::vector<double>        dblvec2 = { 0.998, 0.3456, 0.056, 0.15678, 0.00345,
                                       0.923, 0.06743, 0.1 };
std::vector<std::string>   strvec = { "Insight", "John Dow", "Alakazam",
                                      "Persian Prince", "Bugs Bunny" };
std::vector<unsigned long> ulgvec = { 1UL, 2UL, 3UL, 4UL, 5UL, 8UL, 7UL, 6UL }
std::vector<unsigned long> xulgvec = ulgvec;

// This is only one way of loading data into a DataFrame instance. There are
// many different ways of doing it. Please see the documentation,
// or dataframe_tester.cc
int rc = df.load_data(std::move(ulgvec),  // Index column
                      std::make_pair("int_col", intvec),
                      std::make_pair("dbl_col", dblvec),
                      std::make_pair("dbl_col_2", dblvec2),
                      std::make_pair("str_col", strvec),
                      std::make_pair("ul_col", xulgvec));

// This is another way of loading a DataFrame
MyDataFrame       df2;
std::future<bool> fut = df2.read_async("sample_data.csv", io_format::csv);

fut.get();
        
// Sort the Frame by index
df.sort<MyDataFrame::TimeStamp, int, double, std::string>();
// Sort the Frame by column “dbl_col_2”
df.sort<double, int, double, std::string>("dbl_col_2");

// A functor to calculate mean, variance, skew, kurtosis, defined in
// DataFrameVisitors.h file.
// You can implement your own algorithms and extend the DataFrame easily 
StatsVisitor<double>  stats_visitor;
// Calculate the stats on column “dbl_col”
df.visit<double>("dbl_col", stats_visitor);
``` 

---

## [DataFrame Documentation](docs/DataFrameDoc.pdf)
[DateTime Documentation](docs/DateTimeDoc.pdf)

---

## [DataFrame Test File](test/dataframe_tester.cc)
[Heterogeneous Vectors Test File](test/vectors_tester.cc)<BR>
[Date/Time Test File](test/date_time_tester.cc)
  
---

[Contributions](docs/CONTRIBUTING.md)<BR>
[License](License)

---

### Installing using CMake
```bash
mkdir [Debug | Release]
cd [Debug | Release]
cmake -DCMAKE_BUILD_TYPE=[Debug | Release] ..
make
make install
```

### Uninstalling

```bash
cd [Debug | Release]
make uninstall
```
---

### Performance
There is a test program [dataframe_performance](test/dataframe_performance.cc) that should give you some sense of how this library performs. As a comparison, there is also a Pandas Python [pandas_performance](test/pandas_performance.py) script that does exactly the same thing.<BR>
dataframe_performance.cc is compiled with gcc compiler with -O3 flag.<BR>
pandas_performance.py is ran with Python 3.7.<BR>
I ran both on my mac-book, doing the following:<BR> 
<img src="docs/MacSize.png" alt="drawing" width="500"/>

1. Generate ~1.6 billion second resolution timestamps and load it into the DataFrame/Pandas as index.
2. Generate ~1.6 billion random numbers each for 3 columns with normal, log normal, and exponential distributions and load them into the DataFrame/Pandas.
3. Calculate the mean of each of the 3 columns.

Result:
```bash
Prompt> time python pandas_performance.py
All memory allocations are done. Calculating means ...

real  17m18.916s
user  4m47.113s
sys   5m31.901s
Prompt>
Prompt>
Prompt> time ../bin/Linux.GCC64/dataframe_performance
All memory allocations are done. Calculating means ...

real  6m40.222s
user  2m54.362s
sys   2m14.951s
```
The interesting part:<BR>
In case of Pandas allocating memory and calculating means take almost the same time.<BR>
In case of DataFrame 85% of the time is spent in allocating memory.
