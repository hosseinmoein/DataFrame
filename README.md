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
[![Build Status](https://travis-ci.org/hosseinmoein/DataFrame.svg?branch=master)](https://travis-ci.org/hosseinmoein/DataFrame)<BR>
![GitHub](https://img.shields.io/github/license/hosseinmoein/DataFrame.svg?color=red&style=popout)
![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/hosseinmoein/DataFrame.svg?color=blue&label=Official%20Release&style=popout)<BR>
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard )
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/db646376a4014c3788c7224e670fe451)](https://app.codacy.com/manual/hosseinmoein/DataFrame?utm_source=github.com&utm_medium=referral&utm_content=hosseinmoein/DataFrame&utm_campaign=Badge_Grade_Dashboard)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hosseinmoein/DataFrame/master)
<!--
[![HitCount](http://hits.dwyl.io/hosseinmoein/DataFrame.svg)](http://hits.dwyl.io/hosseinmoein/DataFrame)
-->

<img src="docs/pandalion.png" alt="drawing" width="500"/>

# DataFrame
This is a C++ statistical library that provides an interface similar to Pandas package in Python.<BR>
<B>A DataFrame can have one index column and many data columns of any built-in or user-defined type</B>.<BR>
You could slice the data in many different ways. You could join, merge, group-by the data. You could run various statistical, summarization and ML algorithms on the data. You could add your custom algorithms easily. You could multi-column sort, custom pick and delete the data. And more …<BR>

<B>I have followed a few principles in this library:</B><BR>

1.  Support any type either built-in or user defined without needing new code<BR>
2.  Never chase pointers ala `linked lists`, `std::any`, `pointer to base`, ..., including `virtual function calls`<BR>
3.  Have all column data in continuous memory space. Also, be mindful of cache-line aliasing misses between multiple columns<BR>
4.  Never use more space than you need ala `unions`, `std::variant`, ...<BR>
5.  Avoid copying data as much as possible. Unfortunately, sometimes you have to<BR>
6.  Use multi-threading but only when it makes sense<BR>
7.  Do not attempt to protect the user against `garbage in, garbage out`<BR>

<B>Views</B><BR>
-    You can slice the data frame and instead of getting another data frame you can opt to get a view. A view is a data frame that is a reference to a slice of the original data frame. So if you change the data in the view the corresponding data in the original data frame will also be changed (and vice versa).<BR>

<B>Multithreading</B><BR>
1.  DataFrame uses static containers to achieve type heterogeneity. By default, these static containers are unprotected. This is done by design. So by default, there is no locking overhead. If you use DataFrame in a multithreaded program you must provide a _SpinLock_ defined in <a href="include/DataFrame/Utils/ThreadGranularity.h">ThreadGranularity.h</a> file. DataFrame will use your _SpinLock_ to protect the containers.<BR>Please see <a href="https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html">documentation</a>, <a href="https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/remove_lock.html">set_lock()</a>, <a href="https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/remove_lock.html">remove_lock()</a>, and <a href="test/dataframe_tester.cc#L3768">dataframe_tester.cc</a> for code example.<BR>
2.  In addition, instances of DataFrame are not multithreaded safe either. In other words, a single instance of DataFrame must not be used in multiple threads without protection, unless it is used as read-only.<BR>
3.  In the meantime, DataFrame utilizes multithreading in two different ways internally:<BR>
    1.  <B>Async Interface:</B> There are asynchronous versions of some methods. For example, you have _sort()_/_sort_async()_, _visit()_/_visit_async()_, ... more. The latter versions return a _std::future_ that could execute in parallel.<BR>
    2.  DataFrame uses multiple threads, internally and unbeknown to the user, in some of its algorithms when appropriate. User can control (or turn off) the multithreading by calling _set_thread_level()_ which sets the max number of threads to be used. The default is 0. The optimal number of threads is a function of users hardware/software environment and usually obtained by trail and error. _set_thread_level()_ and threading level in general is a static property and once set, it applies to all instances.<BR>

<B><a href="docs/DateTimeDoc.pdf">DateTime</a></B><BR>
-    DateTime class included in this library is a very cool and handy object to manipulate date/time with nanosecond precision.<BR>

---

## [DataFrame Documentation / Code Samples](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html)
<!--
## [DataFrame Documentation](docs/DataFrameDoc.pdf)
-->
[DateTime Documentation](docs/DateTimeDoc.pdf)

---

[DataFrame Test File](test/dataframe_tester.cc)<BR>
[DataFrame Test File 2](test/dataframe_tester_2.cc)<BR>
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

### Package managers
If you are using Conan to manage your dependencies, merely add `dataframe/x.y.z@` to your requires, where x.y.z is the release version you want to use. Conan will acquire DataFrame, build it from source in your computer, and provide CMake integration support for your projects. See the [conan docs](https://docs.conan.io/en/latest/) for more information.<BR> Sample `conanfile.txt`:

```text
[requires]
dataframe/1.7.0@

[generators]
cmake
```
---

### Performance
There is a test program [_dataframe_performance_](test/dataframe_performance.cc) that should give you some sense of how this library performs. As a comparison, there is also a Pandas Python [_pandas_performance_](test/pandas_performance.py) script that does exactly the same thing.<BR>
_dataframe_performance.cc_ uses DataFrame async interface and is compiled with gcc compiler with -O3 flag.<BR>
_pandas_performance.py_ is ran with Pandas 1.1.0 and Python 3.7.<BR>
I ran both on my mac-book, doing the following:<BR>
<img src="docs/MacSize.png" alt="drawing" width="500"/>

1.  Generate ~1.6 billion second resolution timestamps and load it into the DataFrame/Pandas as index.<BR>
2.  Generate ~1.6 billion random numbers each for 3 columns with normal, log normal, and exponential distributions and load them into the DataFrame/Pandas.<BR>
3.  Calculate the mean of each of the 3 columns.<BR>

Result:
```bash
MacBook> time python test/pandas_performance.py
All memory allocations are done. Calculating means ...

real  17m18.916s
user  4m47.113s
sys   5m31.901s


MacBook> time bin/Linux.GCC64/dataframe_performance
All memory allocations are done. Calculating means ...

real  6m40.222s
user  2m54.362s
sys   2m14.951s
```
<B>The interesting part:</B><BR>
1.  Pandas script, I believe, is entirely implemented in Numpy which is in C.<BR>
2.  In case of Pandas, allocating memory + random number generation takes almost the same amount of time as calculating means.<BR>
3.  In case of DataFrame 85% of the time is spent in allocating memory + random number generation.<BR>
4.  You load data once, but calculate statistics many times. So DataFrame, in general, is about 8x faster than parts of Pandas that are implemented in Numpy. I leave parts of Pandas that are purely in Python to imagination.<BR>
5.  Pandas process image at its peak is ~105GB. C++ DataFrame process image at its peak is ~68GB.
