[![Build status](https://ci.appveyor.com/api/projects/status/hjw01qui3bvxs8yi?svg=true)](https://ci.appveyor.com/project/justinjk007/dataframe)
[![Build Status](https://travis-ci.org/justinjk007/DataFrame.svg?branch=master)](https://travis-ci.org/justinjk007/DataFrame)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard )

![Alt text](docs/pandalion.png "DataFrame")

# DataFrame
This is a C++ statistical library that provides an interface similar to Pandas package in Python.<BR>
You could do almost everything you could do with Pandas within the C++ syntax and type checking framework. You can add/delete any data column type, slice, run summarization functors, transpose, etc. like Pandas. But I also added something that Pandas doesn’t have; <B>Views</B>. You can slice the data frame and instead of getting another data frame you can opt to get a view. A view is a data frame that is a reference to a slice of the original data frame. So if you change the data in the view the corresponding data in the original data frame will also be changed.<BR>


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
