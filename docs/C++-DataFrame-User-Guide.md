# C++ DataFrame Library - Comprehensive Documentation

**Title:** C++ DataFrame Library - Comprehensive Documentation  
**Description:** Complete documentation for C++ DataFrame library for statistical analysis, machine learning, financial data processing, and data science applications  
**Keywords:** C++ DataFrame, data analysis, statistical computing, machine learning, financial analysis, heterogeneous container, templated library, data science, quantitative analysis, time series  
**Version:** Latest  
**Repository:** [https://github.com/hosseinmoein/DataFrame](https://github.com/hosseinmoein/DataFrame)  
**Website:** [https://hosseinmoein.github.io/DataFrame/](https://hosseinmoein.github.io/DataFrame/)  
**License:** BSD-3-Clause  
**Language:** C++23  
**Platform:** Cross-platform (Linux, Windows, macOS)

---

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
   - [What is C++ DataFrame](#11-what-is-c-dataframe)
   - [Core Features and Capabilities](#12-core-features-and-capabilities)
   - [Data Structure and Schema](#13-data-structure-and-schema)
   - [Key Characteristics](#14-key-characteristics)

2. [Library Architecture](#2-library-architecture)
   - [Template Parameters](#21-template-parameters)
   - [Index Column Concept](#22-index-column-concept)
   - [Heterogeneous Vector Types](#23-heterogeneous-vector-types)
   - [Memory Alignment Options](#24-memory-alignment-options)
   - [DataFrame Views](#25-dataframe-views)

3. [DateTime Support](#3-datetime-support)
   - [DateTime Class Overview](#31-datetime-class-overview)
   - [Features and Capabilities](#32-features-and-capabilities)

4. [API Reference](#4-api-reference)
   - [API Categories](#41-api-categories)
   - [Data Access and Manipulation](#42-data-access-and-manipulation)
   - [Analytical Algorithms and Visitors](#43-analytical-algorithms-and-visitors)

5. [Multithreading and Performance](#5-multithreading-and-performance)
   - [Overview](#51-overview)
   - [User Multithreading](#52-user-multithreading)
   - [DataFrame Internal Multithreading](#53-dataframe-internal-multithreading)
   - [Thread Pool Configuration](#54-thread-pool-configuration)
   - [Asynchronous Interfaces](#55-asynchronous-interfaces)

6. [Visitors Pattern](#6-visitors-pattern)
   - [Concept and Purpose](#61-concept-and-purpose)
   - [Implementation Details](#62-implementation-details)

7. [Memory Alignment and SIMD Optimization](#7-memory-alignment-and-simd-optimization)
   - [Custom Memory Alignment](#71-custom-memory-alignment)
   - [SIMD Instructions Support](#72-simd-instructions-support)
   - [Performance Optimization](#73-performance-optimization)
   - [Cache Line Optimization](#74-cache-line-optimization)

8. [Numeric Generators](#8-numeric-generators)
   - [Random Number Generation](#81-random-number-generation)
   - [Distribution Support](#82-distribution-support)
   - [Usage Examples](#83-usage-examples)

9. [Build Instructions](#9-build-instructions)
   - [Build Types](#91-build-types)
   - [CMake Build System](#92-cmake-build-system)
   - [Package Manager Installation](#93-package-manager-installation)
   - [Alternative Build Methods](#94-alternative-build-methods)
   - [Testing and Validation](#95-testing-and-validation)

---

## 1. Introduction and Overview

### 1.1 What is C++ DataFrame

DataFrame is a high-performance, templatized, and heterogeneous C++ container specifically designed for data analysis in statistical, machine-learning, and financial applications.

#### Primary Use Cases

- Statistical data analysis and processing
- Machine learning data preparation and feature engineering
- Financial time series analysis and algorithmic trading
- Quantitative research and analytics
- Scientific computing and data science workflows
- Big data processing in C++ applications

#### Data Structure Paradigm

Think of DataFrame as a two-dimensional data structure of columns and rows, similar to:

- Excel spreadsheet
- SQL database table
- Python Pandas DataFrame
- R data.frame

However, C++ DataFrame extends beyond two dimensions. Columns can contain vectors of any type, including other DataFrames or custom containers, enabling multi-dimensional data structures.

### 1.2 Core Features and Capabilities

#### Column-First Design

Columns are first-class citizens in DataFrame. Operations and access to columns are significantly more efficient and easier than row-by-row data manipulation. This design choice optimizes for:

- Vectorized operations
- Cache-efficient processing
- SIMD instruction utilization
- Parallel computation

#### Intuitive Analytics API

The API is designed to be:

- Open-ended and extensible
- Easy to integrate custom algorithms
- Consistent across different operations
- Type-safe at compile time

### 1.3 Data Structure and Schema

#### Schema Management

Every DataFrame inherently includes a schema that can be:

- Built dynamically at runtime
- Loaded from external files
- Serialized and transmitted across network nodes
- Reconstructed on remote systems

#### Distribution and Serialization

DataFrame supports sharing between different nodes (computers) through:

- File-based persistence
- In-memory serialization to buffers
- Network transmission and reconstruction

### 1.4 Key Characteristics

#### Structural Properties

- Single index column with multiple data columns
- Supports any built-in or user-defined types
- Each data column can be at most as long as the index column
- Columns maintain creation order (modifiable through rotation)
- Column access requires compile-time type knowledge

#### Type Safety

To access a column for any operation, you must know:

- Column name (or index position)
- Column data type at compile time

#### Getting Started Resources

- Hello World examples
- Cheat Sheet for common operations
- DataFrame Library Types reference

#### Interfaces

- Synchronous methods for immediate execution
- Asynchronous methods returning `std::future`
- Thread-safe operations (with proper configuration)

**IMPORTANT:** Before production use, thoroughly review these sections:

- Multithreading configuration and best practices
- Views and memory management
- Visitors pattern for analytical operations
- Memory alignment for performance optimization

---

## 2. Library Architecture

### 2.1 Template Parameters

#### DataFrame Class Definition

```cpp
template<typename I, typename H>
class DataFrame;
```

#### Template Parameter I (Index Type)

Specifies the index column type. The index in DataFrame differs from SQL database indices:

**SQL Database Index:**
- Makes data access efficient
- Doesn't provide additional information
- Used primarily for lookup optimization

**DataFrame Index Column:**
- Metadata describing each row
- Provides contextual information about data points
- Examples: timestamps, frequencies, measurement conditions
- Can be composite structures (e.g., struct with temperature, altitude, etc.)

#### Template Parameter H (Heterogeneous Vector)

Specifies the heterogeneous vector type for containing DataFrame columns.

**Note:** Don't get overwhelmed by H's complexity. Use the convenient typedef's provided in DataFrame Library Types documentation.

### 2.2 Index Column Concept

The index column serves as metadata providing context for each row:

- **Time series:** timestamps or date-time values
- **Frequency domain:** frequency bins or wavelengths
- **Experimental data:** measurement conditions (temperature, pressure, etc.)
- **Multi-dimensional:** composite descriptors in custom structs

### 2.3 Heterogeneous Vector Types

#### H Template Parameter Options

**1. HeteroVector<std::size_t A = 0>**
- Actual heterogeneous vector containing data
- Results in a "standard" DataFrame
- Owns the data
- Full modification capabilities

**2. HeteroPtrView<std::size_t A = 0>**
- Heterogeneous vector view
- Non-contiguous view into another DataFrame slice
- Disjointed data access
- Lightweight reference

**3. HeteroConstPtrView<std::size_t A = 0>**
- Const version of HeteroPtrView
- Read-only access
- Prevents data modification

**4. HeteroView<std::size_t A = 0>**
- Heterogeneous vector view
- Contiguous slice of another DataFrame
- More efficient than HeteroPtrView
- Sequential data access

**5. HeteroConstView<std::size_t A = 0>**
- Const version of HeteroView
- Read-only contiguous access

### 2.4 Memory Alignment Options

#### Template Parameter A

Specifies byte boundary alignment for memory allocations.

**Default Behavior:**
- `A = 0`: Uses system default alignment boundaries for each type

**Custom Alignment:**
- Enables SIMD instruction optimization
- Prevents false cache-line sharing
- Improves performance for vectorized operations

See sections:
- DataFrame Library Types for convenient typedef's
- Memory Alignment section for detailed information

### 2.5 DataFrame Views

DataFrame methods may return:
- New DataFrame instances
- View instances (HeteroPtrView, HeteroView, etc.)

#### User Perspective

DataFrame and view instances are designed to be indistinguishable from the user's perspective. Views provide:

- Efficient data slicing without copying
- Consistent API across DataFrame types
- Transparent access patterns
- Memory-efficient operations

#### Implementation Details

**Standard DataFrame:**
- Owns column data
- Data stored in HeteroVector
- Full modification rights

**View DataFrame:**
- References data from another DataFrame
- Data stored in HeteroView or HeteroPtrView
- Limited ownership semantics

**Important Considerations:**

1. **View Lifetime Management:**
   - Views don't own underlying data
   - Original DataFrame must outlive all its views
   - Dangling references if original is destroyed

2. **View Safety:**
   - Be cautious returning views from functions
   - Ensure source DataFrame lifetime
   - Consider copying for long-term storage

3. **Performance Trade-offs:**
   - Views avoid copying (efficient for large data)
   - Views add indirection overhead
   - Choose based on use case

---

## 3. DateTime Support

### 3.1 DateTime Class Overview

DataFrame includes a comprehensive DateTime class for temporal data handling with timezone support.

#### Design Philosophy

- Implemented in `DateTime.h`
- Timezone-aware temporal calculations
- Extensive formatting options
- High precision time handling

### 3.2 Features and Capabilities

#### Core Functionality

- Date and time arithmetic
- Timezone conversions
- Parsing from multiple formats
- Formatting to various output styles
- Period calculations
- Business day logic

#### Use Cases

- Financial time series analysis
- Market data timestamps
- Trading session management
- Business calendar operations
- Historical data indexing

---

## 4. API Reference

### 4.1 API Categories

The DataFrame API is organized into several functional categories:

1. **Construction and Initialization**
2. **Data Access and Retrieval**
3. **Data Manipulation and Modification**
4. **Analytical Operations**
5. **I/O Operations**
6. **View Creation**
7. **Statistical Functions**

### 4.2 Data Access and Manipulation

#### Column Operations

- Adding columns
- Removing columns
- Renaming columns
- Rotating column order
- Type-safe column access

#### Row Operations

- Appending rows
- Removing rows
- Sorting operations
- Filtering and selection
- Sampling

#### Joining and Merging

- Inner join
- Outer join
- Left join
- Right join
- Concatenation

### 4.3 Analytical Algorithms and Visitors

DataFrame includes extensive analytical capabilities through the visitor pattern:

#### Statistical Visitors

- Mean, median, mode
- Standard deviation, variance
- Quantiles and percentiles
- Correlation and covariance
- Skewness and kurtosis

#### Time Series Visitors

- Moving averages (SMA, EMA, WMA)
- Bollinger bands
- Rate of change
- Momentum indicators
- Trend analysis

#### Machine Learning Visitors

- Linear regression
- Polynomial fitting
- Principal Component Analysis (PCA)
- K-Nearest Neighbors (KNN)
- Clustering algorithms

#### Financial Visitors

- Sharpe ratio
- Maximum drawdown
- Return calculations
- Risk metrics
- Trading indicators (RSI, MACD, Stochastic)

---

## 5. Multithreading and Performance

### 5.1 Overview

DataFrame provides extensive multithreading support for:

- Large dataset processing
- Parallel analytical operations
- Concurrent I/O operations
- Asynchronous computations

### 5.2 User Multithreading

#### Thread Safety Considerations

DataFrame is thread-safe when:
- Multiple threads read from the same DataFrame
- Each thread works on separate DataFrame instances
- Proper synchronization for concurrent writes

#### Best Practices

1. Use const references for read-only access
2. Avoid shared mutable state
3. Leverage asynchronous methods for parallel work
4. Configure thread pool appropriately

### 5.3 DataFrame Internal Multithreading

#### Automatic Parallelization

DataFrame automatically parallelizes operations when:
- Dataset size exceeds configured threshold
- Operation is parallelizable
- Thread pool is available

#### Parallelized Operations

- Column-wise calculations
- Statistical computations
- Data transformations
- I/O operations
- Sorting and searching

### 5.4 Thread Pool Configuration

#### Thread Pool Settings

Configure the internal thread pool:

```cpp
// Set number of threads
ThreadGranularity::set_thread_level(8);

// Get current thread count
auto count = ThreadGranularity::get_thread_level();
```

#### Granularity Control

- Balance between parallelization overhead and performance gain
- Adjust based on dataset size and operation complexity
- Monitor performance for optimal configuration

### 5.5 Asynchronous Interfaces

#### Async Method Pattern

Many DataFrame methods have asynchronous variants:

```cpp
// Synchronous
auto result = df.mean<double>("column");

// Asynchronous
auto future = df.mean_async<double>("column");
auto result = future.get();
```

#### Benefits

- Non-blocking operations
- Parallel task execution
- Better resource utilization
- Improved application responsiveness

---

## 6. Visitors Pattern

### 6.1 Concept and Purpose

The visitor pattern in DataFrame enables:

- Extensible analytical operations
- Type-safe algorithm application
- Reusable statistical computations
- Custom algorithm integration

#### Design Benefits

1. **Separation of Concerns:** Algorithms separate from data structure
2. **Type Safety:** Compile-time type checking
3. **Reusability:** Same visitor across different DataFrames
4. **Extensibility:** Easy to add custom visitors

### 6.2 Implementation Details

#### Visitor Interface

Visitors implement a standard interface for processing DataFrame columns:

```cpp
template<typename T>
struct MyVisitor {
    using value_type = T;
    using result_type = /* return type */;
    
    void operator()(const T& value);
    result_type get_result() const;
};
```

#### Using Visitors

```cpp
// Create visitor instance
MeanVisitor<double> mean_calc;

// Apply to DataFrame column
df.visit<double>("column_name", mean_calc);

// Get result
auto mean = mean_calc.get_result();
```

#### Custom Visitors

Create custom analytical operations by implementing the visitor interface:

1. Define value_type and result_type
2. Implement operator() for data processing
3. Implement get_result() for returning results
4. Optionally implement pre() and post() hooks

---

## 7. Memory Alignment and SIMD Optimization

### 7.1 Custom Memory Alignment

#### Purpose

Custom memory alignment enables:

- SIMD instruction utilization
- Cache line optimization
- False sharing prevention
- Performance enhancement

#### Configuration

```cpp
// 64-byte aligned DataFrame
using DF64 = DataFrame<unsigned long, HeteroVector<64>>;

// 128-byte aligned DataFrame
using DF128 = DataFrame<unsigned long, HeteroVector<128>>;
```

### 7.2 SIMD Instructions Support

#### SIMD Benefits

- Parallel data processing
- Vectorized operations
- Significant performance improvements
- Efficient CPU utilization

#### Requirements

- Properly aligned memory
- SIMD-capable CPU
- Compiler optimization flags
- Appropriate data types

### 7.3 Performance Optimization

#### Alignment Strategy

Choose alignment based on:

- CPU architecture (AVX, AVX2, AVX-512)
- Cache line size
- Operation patterns
- Data types

#### Modern CPUs

- Extensive SIMD instruction sets (SSE, AVX, AVX-512)
- Variable cache line widths
- Support for multiple alignment boundaries

#### Alignment Options

Convenient typedef's define DataFrames with different alignment boundaries:

- 64-byte alignment
- 128-byte alignment
- 256-byte alignment
- Custom alignments

**Optimal Alignment:** Depends on your system's cache line width. Experiment for best results.

### 7.4 Cache Line Optimization

#### False Sharing Prevention

Custom alignment can prevent false cache-line sharing between multiple columns.

#### Performance Impact

- Reduced cache invalidation
- Better memory access patterns
- Improved multi-core performance

#### Access Pattern

When accessing columns, you receive a reference to `StlVecType` (`std::vector` with custom allocator for requested alignment).

**Resources:** See DataFrame Library Types documentation for alignment typedef's.

---

## 8. Numeric Generators

### 8.1 Random Number Generation

#### Purpose

Convenient standalone functions for generating random numbers with various statistical distributions.

#### Integration

Seamlessly generate random DataFrame columns with these utilities.

### 8.2 Distribution Support

#### Features

- Multiple probability distributions
- Space-optimized result vectors
- Configurable memory alignment
- High-performance generation

#### Supported Distributions

- Uniform distribution
- Normal (Gaussian) distribution
- Exponential distribution
- Bernoulli distribution
- Binomial distribution
- Poisson distribution
- And more...

### 8.3 Usage Examples

#### Documentation Resources

- Dedicated generator documentation
- `RandGen.h` header file
- `dataframe_tester.cc` examples
- `RandGenParams` definition in `DataFrameTypes.h`

#### Configuration

See `DataFrameTypes.h` for `RandGenParams` definition and default values.

---

## 9. Build Instructions

### 9.1 Build Types

#### Sanity Checks Configuration

**Compile-time Flag:** `HMDF_SANITY_EXCEPTIONS=1`

**With HMDF_SANITY_EXCEPTIONS Defined:**
- Runtime dimensionality checks
- Data validation
- Explanatory exceptions on errors
- Debugging assistance

**Example Error Scenario:**

Calling KNN with K greater than available data points:
- **With flag:** Descriptive exception thrown
- **Without flag:** Garbage results or crash

**Without HMDF_SANITY_EXCEPTIONS:**
- No runtime checks
- Assumes bug-free code
- Maximum performance
- Risk of mysterious crashes

**Recommendation:** Always use `HMDF_SANITY_EXCEPTIONS` during development and testing. Only disable in thoroughly tested production code.

#### Build Configuration Types

**1. Debug Build:**
- Debug information included
- No optimizations
- Larger executable files
- Significantly slower execution
- Full debugging capabilities

**2. Release Build:**
- Full compiler optimizations
- No debug information
- Smaller executable files
- Significantly faster execution
- Limited crash diagnostics

**3. Custom Build:**
- User-defined optimization levels
- Custom debug information
- Experimental configurations

### 9.2 CMake Build System

#### Repository Setup

**Clone Repository:**

```bash
git clone https://github.com/hosseinmoein/DataFrame.git
```

#### CMake Build Process

**Create Build Directory:**

```bash
mkdir [Debug|Release]
cd [Debug|Release]
```

**Release Build (Optimized):**

Without Sanity Checks:

```bash
cmake -DCMAKE_BUILD_TYPE=Release \
      -DHMDF_BENCHMARKS=1 \
      -DHMDF_EXAMPLES=1 \
      -DHMDF_TESTING=1 ..
```

With Sanity Checks:

```bash
cmake -DCMAKE_BUILD_TYPE=Release \
      -DHMDF_SANITY_EXCEPTIONS=1 \
      -DHMDF_BENCHMARKS=1 \
      -DHMDF_EXAMPLES=1 \
      -DHMDF_TESTING=1 ..
```

**Debug Build:**

```bash
cmake -DCMAKE_BUILD_TYPE=Debug \
      -DHMDF_SANITY_EXCEPTIONS=1 \
      -DHMDF_BENCHMARKS=1 \
      -DHMDF_EXAMPLES=1 \
      -DHMDF_TESTING=1 ..
```

**Compile and Install:**

```bash
make
make install
```

**Uninstall:**

```bash
cd [Debug|Release]
make uninstall
```

### 9.3 Package Manager Installation

#### Conan Platform

DataFrame is available on Conan package manager.  
See [Conan documentation](https://conan.io/) for installation and usage instructions.

#### VCPKG Platform

DataFrame is available on VCPKG package manager.  
See [VCPKG documentation](https://vcpkg.io/) for installation and usage instructions.

### 9.4 Alternative Build Methods

#### Plain Make (Not Recommended)

**Process:**
1. Navigate to src subdirectory
2. Execute `build_all.sh` script
3. Builds library and test executables

**Limitations:**
- Linux/Unix flavors only
- Less flexible than CMake
- Manual dependency management

### 9.5 Testing and Validation

#### Test Data Requirements

**Location:**

Test programs in `test/`, `examples/`, and `benchmarks/` directories require mock data files from the `data/` directory.

**Important:**
- Data files must be in current directory when running tests
- CMake automatically copies data files to execution directory
- Manual execution requires `data/` as current directory

**Test Execution:**

If running tests manually, ensure `data/` directory is your current working directory.

---

## Additional Resources

### Documentation

- **Complete API Reference:** [https://hosseinmoein.github.io/DataFrame/docs/HTML/DataFrame.html](https://hosseinmoein.github.io/DataFrame/docs/HTML/DataFrame.html)
- **GitHub Repository:** [https://github.com/hosseinmoein/DataFrame](https://github.com/hosseinmoein/DataFrame)
- **Issue Tracker:** [https://github.com/hosseinmoein/DataFrame/issues](https://github.com/hosseinmoein/DataFrame/issues)
- **Discussions:** [https://github.com/hosseinmoein/DataFrame/discussions](https://github.com/hosseinmoein/DataFrame/discussions)

### Quick Start

- Hello World Examples
- Cheat Sheet PDF
- Sample Code in `examples/` directory
- Test Suite in `test/` directory

### Support

- GitHub Issues for bug reports
- GitHub Discussions for questions
- Documentation for comprehensive guides

### Contributing

- Pull requests welcome
- See contribution guidelines
- Test coverage required
- Documentation updates appreciated

---

## Keywords for Search Optimization

### Primary Keywords

C++ DataFrame, data analysis C++, statistical computing C++, machine learning C++, financial data analysis, time series analysis C++, quantitative analysis, heterogeneous container C++, templated DataFrame, data science C++

### Secondary Keywords

pandas alternative C++, R dataframe C++, columnar data structure, vectorized operations, SIMD optimization, parallel computing C++, thread-safe DataFrame, high-performance analytics, big data C++, numerical computing, statistical library C++, trading algorithms

### Technical Keywords

template metaprogramming, visitor pattern, memory alignment, cache optimization, multithreading C++, async operations, compile-time type safety, heterogeneous storage, STL containers, modern C++, C++20, C++23

### Application Domains

quantitative finance, algorithmic trading, data science, machine learning pipeline, statistical analysis, time series forecasting, financial engineering, risk analysis, backtesting, market data analysis

---

**Last Updated:** 2024  
**For latest version, visit:** [https://github.com/hosseinmoein/DataFrame](https://github.com/hosseinmoein/DataFrame)
