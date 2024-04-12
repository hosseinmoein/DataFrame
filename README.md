<!--
Copyright (c) 2019-2026, Hossein Moein
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
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://isocpp.org/std/the-standard )
[![Build status](https://ci.appveyor.com/api/projects/status/hjw01qui3bvxs8yi?svg=true)](https://ci.appveyor.com/project/hosseinmoein/dataframe)
<BR>
[![GitHub](https://img.shields.io/github/license/hosseinmoein/DataFrame.svg?color=red&style=popout)](https://github.com/hosseinmoein/DataFrame/blob/master/License)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/db646376a4014c3788c7224e670fe451)](https://app.codacy.com/organizations/gh/hosseinmoein/repositories)
<BR>
[![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/hosseinmoein/DataFrame.svg?color=blue&label=Official%20Release&style=popout)](https://github.com/hosseinmoein/DataFrame/releases)
[![Conan Center](https://img.shields.io/conan/v/dataframe)](https://conan.io/center/recipes/dataframe)
[![VCPKG package](https://repology.org/badge/version-for-repo/vcpkg/dataframe.svg)](https://vcpkg.link/ports/dataframe)

<img src="docs/LionLookingUp.jpg" alt="DataFrame Lion" width="400" longdesc="https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html"/>

## <a href="https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html" target="_blank"><B>DataFrame documentation with code samples</B></a>
This is a C++ analytical library designed for data analysis similar to libraries in Python and R. For example, you would compare this to [Pandas](https://pandas.pydata.org), [R data.frame](https://www.w3schools.com/r/r_data_frames.asp), or [Polars](https://www.pola.rs) <BR>
You can slice the data in many different ways. You can join, merge, group-by the data. You can run various statistical, summarization, financial, and ML algorithms on the data. You can add your custom algorithms easily. You can multi-column sort, custom pick and delete the data. And more …<BR>
DataFrame also includes a large collection of analytical algorithms in form of visitors. These are from basic stats such as <I>Mean</I>, <I>Std Deviation</I>, <I>Return</I>, … to more involved analysis such as <I>Affinity Propagation</I>, <I>Polynomial Fit</I>, <I>Fast Fourier transform of arbitrary length</I> … including a good collection of trading indicators. You can also easily add your own algorithms.<BR>
DataFrame also employs extensive multithreading in almost all its API’s, for large datasets. That makes DataFrame especially suitable for analyzing large datasets.<BR>
For basic operations to start you off, see [Hello World](examples/hello_world.cc). For a complete list of features with code samples, see <a href="https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/DataFrame.html" target="_blank">documentation</a>.

I have followed a few <B>principles in this library</B>:<BR>

1. [Support any type either built-in or user defined without needing new code](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/any_type.html)
2. [Never chase pointers ala _linked lists_, _std::any_, _pointer to base_, ...](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/pointers.html)
3. [Have all column data in contiguous memory space](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/contiguous_memory.html)
4. [Never use more space than you need ala _unions_, _std::variant_, ...](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/std_variant.html)
5. [Avoid copying data as much as possible](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/copying_data.html)
6. [Use multi-threading but only when it makes sense](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/multithreading.html)
7. [Do not attempt to protect the user against _garbage in_, _garbage out_](https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/garbage_in_garbage_out.html)

---

### Performance
You have probably heard of Polars DataFrame. It is implemented in Rust and ported with zero-overhead to Python (as long as you don’t have a loop). I have been asked by many people to write a comparison for <B>DataFrame vs. Polars</B>. So, I finally found some time to learn a bit about Polars and write a very simple benchmark.<BR>
I wrote the following identical programs for both Polars and C++ DataFrame. I used Polars version 0.19.14. And I used C++20 clang compiler with -O3 option. I ran both on my, somewhat outdated, MacBook Pro.<BR>
In both cases, I created a dataframe with 3 random columns. The C++ DataFrame also required an additional index column of the same size. Polars doesn’t believe in index columns (that has its own pros and cons. I am not going through it here).
Each program has three identical parts. First it generates and populates 3 columns with 300m random numbers each (in case of C++ DataFrame, it must also generate a sequential index column of the same size). That is the part I am _not_ interested in. In the second part, it calculates the mean of the first column, the variance of the second column, and the Pearson correlation of the second and third columns. In the third part, it does a select (or filter as Polars calls it) on one of the columns.

**Results**:<BR>
The maximum dataset I could load into Polars was 300m rows per column. Any bigger dataset blew up the memory and caused OS to kill it. I ran C++ DataFrame with 10b rows per column and I am sure it would have run with bigger datasets too. So, I was forced to run both with 300m rows to compare.
I ran each test 4 times and took the best time. Polars numbers varied a lot from one run to another, especially calculation and selection times. C++ DataFrame numbers were significantly more consistent.

```text
C++ DataFrame:
    Data generation/load time:  26.945900 secs
    Calculation time:            1.260150 secs
    Selection time:              0.742493 secs
    Overall time:               28.948600 secs

Polars:
    Data generation/load time:  28.468640 secs
    Calculation time:            4.876561 secs
    Selection time:              3.876561 secs
    Overall time:               36.876345 secs

Pandas, for comparison:
    Data generation/load time:  36.678976 secs
    Calculation time:           40.326350 secs
    Selection time:              8.326350 secs
    Overall time:               85.845114 secs
```

[C++ DataFrame source file](https://github.com/hosseinmoein/DataFrame/blob/master/benchmarks/dataframe_performance.cc) <BR>
[Polars source file](https://github.com/hosseinmoein/DataFrame/blob/master/benchmarks/polars_performance.py) <BR>
[Pandas source file](https://github.com/hosseinmoein/DataFrame/blob/master/benchmarks/pandas_performance.py)

---

[**Please consider sponsoring DataFrame, especially if you are using it in production capacity. It is the strongest form of appreciation**](https://github.com/sponsors/hosseinmoein)

---

### Installing using CMake
```sh
mkdir [Debug|Release]
cd [Debug|Release]

cmake -DCMAKE_BUILD_TYPE=Release -DHMDF_BENCHMARKS=1 -DHMDF_EXAMPLES=1 -DHMDF_TESTING=1 ..
cmake -DCMAKE_BUILD_TYPE=Debug -DHMDF_SANITY_EXCEPTIONS=1 -DHMDF_BENCHMARKS=1 -DHMDF_EXAMPLES=1 -DHMDF_TESTING=1 ..

make
make install

cd [Debug|Release]
make uninstall
```

### Package managers
DataFrame is available on [_Conan_](https://conan.io/center/recipes/dataframe) platform. See the [_Conan_ docs](https://docs.conan.io/en/latest/) for more information.<BR>
DataFrame is also available on [_Microsoft VCPKG_](https://vcpkg.link/ports/dataframe) platform. See [_VCPKG docs_](https://learn.microsoft.com/en-us/vcpkg/) for more information<BR>
