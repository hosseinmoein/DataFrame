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
[![Build Status](https://travis-ci.org/hosseinmoein/DataFrame.svg?branch=master)](https://travis-ci.com/hosseinmoein/DataFrame)<BR>
![GitHub](https://img.shields.io/github/license/hosseinmoein/DataFrame.svg?color=red&style=popout)
![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/hosseinmoein/DataFrame.svg?color=blue&label=Official%20Release&style=popout)<BR>
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard )
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/db646376a4014c3788c7224e670fe451)](https://app.codacy.com/manual/hosseinmoein/DataFrame?utm_source=github.com&utm_medium=referral&utm_content=hosseinmoein/DataFrame&utm_campaign=Badge_Grade_Dashboard)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hosseinmoein/DataFrame/master)
<!--
[![HitCount](http://hits.dwyl.io/hosseinmoein/DataFrame.svg)](http://hits.dwyl.io/hosseinmoein/DataFrame)
-->

<img src="docs/LionLookingUp.jpg" alt="drawing" width="400"/>

## [*DataFrame Documentation / Code Samples*](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html)
This is a C++ analytical library that provides interface and functionality similar to packages/libraries in Python and R. For example, you could compare this to Pandas or R data.frame.<BR>
You could slice the data in many different ways. You could join, merge, group-by the data. You could run various statistical, summarization, financial, and ML algorithms on the data. You could add your custom algorithms easily. You could multi-column sort, custom pick and delete the data. And more …<BR>
DataFrame also includes a large collection of analytical algorithms in form of visitors. These are from basic stats such as <I>Mean</I>, <I>Std Deviation</I>, <I>Return</I>, … to more involved analysis such as <I>Affinity Propagation</I>, <I>Polynomial Fit</I>, <I>Fast Fourier transform of arbitrary length</I> … including a good collection of trading indicators. You could also easily add your own algorithms.<BR>
For basic operations to start you off, see [Hello World](test/hello_world.cc). For a complete list of features with code samples, see [documentation](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html).

I have followed a few <B>principles in this library</B>:<BR>

1.  Support any type either built-in or user defined without needing new code
2.  Never chase pointers ala `linked lists`, `std::any`, `pointer to base`, ..., including `virtual functions`
3.  Have all column data in continuous memory space. Also, be mindful of cache-line aliasing misses between multiple columns
4.  Never use more space than you need ala `unions`, `std::variant`, ...
5.  Avoid copying data as much as possible
6.  Use multi-threading but only when it makes sense
7.  Do not attempt to protect the user against `garbage in`, `garbage out`

[DateTime](docs/DateTimeDoc.pdf)<BR>
DateTime class included in this library is a very cool and handy object to manipulate date/time with nanosecond precision and multi timezone capability.<BR>

---

### Performance
There is a test program [_dataframe_performance_](test/dataframe_performance.cc) that should give you a sense of how this library performs. As a comparison, there is also a Pandas [_pandas_performance_](test/pandas_performance.py) script that does exactly the same thing.<BR>
<I>dataframe_performance.cc</I> uses DataFrame <B>async interface</B> and is compiled with gcc (10.3.0) compiler with -O3 flag. <I>pandas_performance.py</I> is ran with Pandas 1.3.2, Numpy 1.21.2 and Python 3.7 on Xeon E5-2667 v2. What the test program roughly does:<BR>

1.  Generate ~1.6 billion timestamps (second resolution) and load them into the DataFrame/Pandas as index.<BR>
2.  Generate ~1.6 billion random numbers for 3 columns with normal, log normal, and exponential distributions and load them into the DataFrame/Pandas.<BR>
3.  Calculate the mean of each of the 3 columns.<BR>

Result:
```bash
$ python3 test/pandas_performance.py
Starting ... 1629817655
All memory allocations are done. Calculating means ... 1629817883
6.166675403767268e-05, 1.6487168460770107, 0.9999539627671375
1629817894 ... Done

real    5m51.598s
user    3m3.485s
sys     1m26.292s

$ Release/bin/dataframe_performance
Starting ... 1629818332
All memory allocations are done. Calculating means ... 1629818535
1, 1.64873, 1
1629818536 ... Done
  
real    3m34.241s                                                                                                                      
user    3m14.250s
sys     0m25.983s
```
<B>The Interesting Part:</B><BR>
1.  Pandas script, I believe, is entirely implemented in Numpy which is in C.
2.  In case of Pandas, allocating memory + random number generation takes almost the same amount of time as calculating means.
3.  In case of DataFrame ~90% of the time is spent in allocating memory + random number generation.
4.  You load data once, but calculate statistics many times. So DataFrame, in general, is about ~11x faster than parts of Pandas that are implemented in Numpy. I leave parts of Pandas that are purely in Python to imagination.
5.  Pandas process image at its peak is ~105GB. C++ DataFrame process image at its peak is ~56GB.

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

cd [Debug | Release]
make uninstall
```

### Package managers
If you are using _Conan_ to manage your dependencies, add `dataframe/x.y.z@` to your requires, where `x.y.z` is the release version you want to use. _Conan_ will acquire DataFrame, build it from source in your computer, and provide CMake integration support for your projects. See the [_Conan_ docs](https://docs.conan.io/en/latest/) for more information.<BR> Sample `conanfile.txt`:

```text
[requires]
dataframe/1.18.0@

[generators]
cmake
```
