<!--
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
names of its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-->
[![Build status](https://ci.appveyor.com/api/projects/status/hjw01qui3bvxs8yi?svg=true)](https://ci.appveyor.com/project/hosseinmoein/dataframe)
[![Build Status](https://travis-ci.org/hosseinmoein/DataFrame.svg?branch=master)](https://travis-ci.org/hosseinmoein/DataFrame)
![GitHub](https://img.shields.io/github/license/hosseinmoein/DataFrame.svg?color=red&style=popout)
![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/hosseinmoein/DataFrame.svg?color=blue&label=Official%20Release&style=popout)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard )
[![HitCount](http://hits.dwyl.io/hosseinmoein/DataFrame.svg)](http://hits.dwyl.io/hosseinmoein/DataFrame)
![Code Grade](https://www.code-inspector.com/project/3758/status/svg)

<img src="docs/pandalion.png" alt="drawing" width="500"/>

# DataFrame
This is a C++ statistical library that provides an interface similar to Pandas package in Python.<BR>
<B>A DataFrame can have one index column and many data columns of any built-in or user-defined type</B>.<BR>
You could slice the data in many different ways. You could join, merge, group-by the data. You could run various statistical, summarization and ML algorithms on the data. You could add your custom algorithms easily. You could multi-column sort, custom pick and delete the data. And more …<BR>

<B>I have followed a few principals in this library:</B><BR>

1. Support any type either built-in or user defined without needing new code
2. Never chase pointers ala `linked lists`, `std::any`, `pointer to base`, ..., including `virtual function calls`
3. Have all column data in continuous memory space
4. Never use more space than you need ala `unions`, `std::variant`, ...
5. Avoid copying data as much as possible. Unfortunately, sometimes you have to
6. Use multi-threading but only when it makes sense
7. Do not attempt to protect the user against `garbage in, garbage out`
<BR>
<B>Views</B><BR>
- You can slice the data frame and instead of getting another data frame you can opt to get a view. A view is a data frame that is a reference to a slice of the original data frame. So if you change the data in the view the corresponding data in the original data frame will also be changed (and vice versa).<BR><BR>

<B>Multithreading</B><BR>
1. DataFrame uses static containers to achieve type heterogeneity. By default, these static containers are unprotected. This is done by design. So by default, there is no locking overhead. If you use DataFrame in a multithreaded program you must provide a _SpinLock_ defined in <a href="include/DataFrame/Utils/ThreadGranularity.h">ThreadGranularity.h</a> file. DataFrame will use your _SpinLock_ to protect the containers.<BR>
Please see <a href="docs/DataFrameDoc.pdf">documentation</a>, _set_lock()_, _remove_lock()_, and <a href="test/dataframe_tester.cc#L3736">dataframe_tester.cc</a> for code example.
2. In addition, instances of DataFrame are not multithreaded safe either. In other words, a single instance of DataFrame must not be used in multiple threads without protection.
3. In the meantime, DataFrame utilizes multithreading in two different ways internally:
    1. There are asynchronous versions of some methods. For example, you have both _sort()_ and _sort_async()_. The latter returns a _std::future_ that could execute in parallel.
    2. DataFrame uses multiple threads, internally and unbeknown to the user, in some of its algorithms when appropriate. User can control (or turn off) the multithreading by calling _set_thread_level()_ which sets the max number of threads to be used. The default is 0. The optimal number of threads is a function of users hardware/software environment and usually obtained by trail and error. _set_thread_level()_ and threading level in general is a static property and once set, it applies to all instances.<BR>

<B><a href="docs/DateTimeDoc.pdf">DateTime</a></B><BR>
- DateTime class included in this library is a very cool and handy object to manipulate date/time with nanosecond precision.

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
df.sort<MyDataFrame::IndexType, int, double, std::string, unsigned long>("INDEX", sort_spec::ascen);
// Sort the Frame by column “dbl_col_2” in descending order
df.sort<double, int, double, std::string, unsigned long>("dbl_col_2", sort_spec::desce);

// A functor to calculate mean, variance, skew, kurtosis, defined in
// DataFrameStatsVisitors.h file.
// You can implement your own algorithms and extend the DataFrame easily
StatsVisitor<double>  stats_visitor;
// Calculate the stats on column “dbl_col”
df.visit<double>("dbl_col", stats_visitor);
```

---

## [DataFrame Documentation](docs/DataFrameDoc.pdf)
[DateTime Documentation](docs/DateTimeDoc.pdf)

---

[DataFrame Test File](test/dataframe_tester.cc)<BR>
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
MacBook> time python pandas_performance.py
All memory allocations are done. Calculating means ...

real  17m18.916s
user  4m47.113s
sys   5m31.901s
MacBook>
MacBook>
MacBook> time ../bin/Linux.GCC64/dataframe_performance
All memory allocations are done. Calculating means ...

real  6m40.222s
user  2m54.362s
sys   2m14.951s
```
<B>The interesting part:</B><BR>
1. Pandas script, I believe, is entirely implemented in Numpy which is in C.
2. In case of Pandas, allocating memory + random number generation takes almost the same amount of time as calculating means.
3. In case of DataFrame 85% of the time is spent in allocating memory + random number generation.
4. You load data once, but calculate statistics many times. So DataFrame, in general, is about 8x faster than parts of Pandas that are implemented in Numpy. I leave parts of Pandas that are purely in Python to imagination.

