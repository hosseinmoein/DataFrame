#include <iostream>
#include <string>
#include <typeinfo>
#include <limits>
#include <cmath>
#include <cassert>

#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameVisitors.h>
#include <DataFrame/DataFrameOperators.h>

using namespace hmdf;

// -----------------------------------------------------------------------------

struct ReplaceFunctor  {

    bool operator() (const unsigned int &idx, double &value)  {

        if (idx == 20180103)  {
            value *= 1000.0;
            count += 1;
        }
        else if (idx == 20180115)  {
            value *= 1000.0;
            count += 1;
        }
        else if (idx == 20180121)  {
            value *= 1000.0;
            count += 1;
        }

        return (true);
    }

    size_t  count { 0 };
};

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    typedef StdDataFrame<unsigned long> MyDataFrame;

    MyDataFrame::set_thread_level(10);

    MyDataFrame         df;
    std::vector<int>    &col0 =
        df.create_column<int>(static_cast<const char *>("col_name"));

    std::vector<int>            intvec = { 1, 2, 3, 4, 5 };
    std::vector<double>         dblvec =
        { 1.2345, 2.2345, 3.2345, 4.2345, 5.2345 };
    std::vector<double>         dblvec2 =
        { 0.998, 0.3456, 0.056, 0.15678, 0.00345, 0.923, 0.06743, 0.1 };
    std::vector<std::string>    strvec =
        { "Col_name", "Col_name", "Col_name", "Col_name", "Col_name" };
    std::vector<unsigned long>  ulgvec =
        { 1UL, 2UL, 3UL, 4UL, 5UL, 8UL, 7UL, 6UL };
    std::vector<unsigned long>  xulgvec = ulgvec;
    const size_t                total_count =
        ulgvec.size() +
        intvec.size() +
        dblvec.size() +
        dblvec2.size() +
        strvec.size() +
        xulgvec.size() +
        9;  // NaNa inserterd

    MyDataFrame::size_type  rc =
        df.load_data(std::move(ulgvec),
                     std::make_pair("int_col", intvec),
                     std::make_pair("dbl_col", dblvec),
                     std::make_pair("dbl_col_2", dblvec2),
                     std::make_pair("str_col", strvec),
                     std::make_pair("ul_col", xulgvec));

    assert(rc == 48);

    df.load_index(ulgvec.begin(), ulgvec.end());
    df.load_column<int>("int_col", { intvec.begin(), intvec.end() },
                        nan_policy::pad_with_nans);
    df.load_column<std::string>("str_col", { strvec.begin(), strvec.end() },
                                nan_policy::pad_with_nans);
    df.load_column<double>("dbl_col", { dblvec.begin(), dblvec.end() },
                           nan_policy::pad_with_nans);
    df.load_column<double>("dbl_col_2", { dblvec2.begin(), dblvec2.end() },
                           nan_policy::dont_pad_with_nans);

    df.append_column<std::string>("str_col", "Additional column");
    df.append_column("dbl_col", 10.56);

    std::vector<int>    ivec = df.get_column<int> ("int_col");

    std::cout << df.get_column<double> ("dbl_col")[2] << std::endl;
    assert(df.get_column<double> ("dbl_col")[2] == 3.2345);

    MeanVisitor<int>    ivisitor;
    MeanVisitor<double> dvisitor;

    assert(df.visit<int>("int_col", ivisitor).get_result() == 1);
    assert(abs(df.visit<double>("dbl_col",
                                dvisitor).get_result() - 3.2345) < 0.00001);

    df.get_column<double>("dbl_col")[5] = 6.5;
    df.get_column<double>("dbl_col")[6] = 7.5;
    df.get_column<double>("dbl_col")[7] = 8.5;
    assert(::abs(df.visit<double>("dbl_col", dvisitor).get_result() -
                 4.83406) < 0.0001);

    std::cout << "Printing integer vector BEFORE making make_consistent ..."
              << std::endl;

    std::vector<double> dvec = df.get_column<double> ("dbl_col");
    std::vector<double> dvec2 = df.get_column<double> ("dbl_col_2");

    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    for (auto iter : dvec2)
        std::cout << iter << " ";
    std::cout << std::endl;

    df.make_consistent<int, double, std::string>();
    std::cout << "Printing integer vector AFTER making make_consistent ..."
              << std::endl;
    dvec = df.get_column<double> ("dbl_col");
    dvec2 = df.get_column<double> ("dbl_col_2");
    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    for (auto iter : dvec2)
        std::cout << iter << " ";
    std::cout << std::endl;

    df.sort<MyDataFrame::IndexType, int, double, std::string>();
    std::cout << "Printing after sorting the index ..." << std::endl;
    dvec = df.get_column<double> ("dbl_col");
    dvec2 = df.get_column<double> ("dbl_col_2");
    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    for (auto iter : dvec2)
        std::cout << iter << " ";
    std::cout << std::endl;

    df.sort<double, int, double, std::string>("dbl_col_2");
    std::cout << "Printing after sorting the dbl_col_2 ..." << std::endl;
    dvec = df.get_column<double> ("dbl_col");
    dvec2 = df.get_column<double> ("dbl_col_2");
    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    for (auto iter : dvec2)
        std::cout << iter << " ";
    std::cout << std::endl;

    MyDataFrame df2 = df.get_data_by_idx<int, double, std::string>({ 3, 5 });

    std::cout << "Printing the second df after get_data_by_idx() ..."
              << std::endl;
    dvec = df2.get_column<double> ("dbl_col");
    dvec2 = df2.get_column<double> ("dbl_col_2");
    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    for (auto iter : dvec2)
        std::cout << iter << " ";
    std::cout << std::endl;

    MyDataFrame df3 = df.get_data_by_loc<int, double, std::string>({ 1, 2 });

    df3.write<std::ostream, int, double, std::string>(std::cout);
    std::cout << "Printing the second df after get_data_by_loc() ..."
              << std::endl;
    dvec = df3.get_column<double> ("dbl_col");
    dvec2 = df3.get_column<double> ("dbl_col_2");
    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    for (auto iter : dvec2)
        std::cout << iter << " ";
    std::cout << std::endl;

    CorrVisitor<double> corr_visitor;

    std::cout << "Correlation between dbl_col and dbl_col_2 is: "
              << df.visit<double, double>("dbl_col",
                                          "dbl_col_2",
                                          corr_visitor).get_result()
              << std::endl;

    StatsVisitor<double>    stats_visitor;

    df.visit<double>("dbl_col", stats_visitor);
    std::cout << std::endl;
    dvec = df.get_column<double> ("dbl_col");
    for (auto iter : dvec)
        std::cout << iter << " ";
    std::cout << std::endl;
    assert(abs(stats_visitor.get_skew() - 0.0396307) < 0.0001);
    assert(abs(stats_visitor.get_kurtosis() - -1.273) < 0.0001);
    assert(abs(stats_visitor.get_mean() - 4.83406) < 0.0001);
    assert(abs(stats_visitor.get_variance() - 6.58781) < 0.0001);

    SLRegressionVisitor<double> slr_visitor;

    df.visit<double, double>("dbl_col", "dbl_col_2", slr_visitor);
    assert(slr_visitor.get_count() == 8);
    assert(abs(slr_visitor.get_slope() - -0.0561415) < 0.00001);
    assert(abs(slr_visitor.get_intercept() - 0.602674) < 0.00001);
    assert(abs(slr_visitor.get_corr() - -0.358381) < 0.00001);
    assert(abs(df.visit<double, double>("dbl_col", "dbl_col_2",
                                        corr_visitor).get_result() -
               -0.358381) < 0.00001);

    std::cout << "\nTesting GROUPBY:\n" << std::endl;

    std::vector<unsigned long>  ulgvec2 =
        { 123450, 123451, 123452, 123450, 123455, 123450, 123449,
          123448, 123451, 123452, 123452, 123450, 123455, 123450,
          123454, 123453, 123456, 123457, 123458, 123459, 123460,
          123441, 123442, 123432, 123433, 123434, 123435, 123436 };
    std::vector<unsigned long>  xulgvec2 = ulgvec2;
    std::vector<int>            intvec2 =
        { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
          15, 20, 22, 23, 24, 25, 30, 33, 34, 35, 36, 40, 45, 46 };
    std::vector<double>         xdblvec2 =
        { 1.2345, 2.2345, 3.2345, 4.2345, 5.2345, 3.0, 0.9999,
          10.0, 4.25, 0.009, 1.111, 8.0, 2.2222, 3.3333,
          11.0, 5.25, 1.009, 2.111, 9.0, 3.2222, 4.3333,
          12.0, 6.25, 2.009, 3.111, 10.0, 4.2222, 5.3333 };
    std::vector<double>         dblvec22 =
        { 0.998, 0.3456, 0.056, 0.15678, 0.00345, 0.923, 0.06743,
          0.1, 0.0056, 0.07865, -0.9999, 0.0111, 0.1002, -0.8888,
          0.14, 0.0456, 0.078654, -0.8999, 0.01119, 0.8002, -0.9888,
          0.2, 0.1056, 0.87865, -0.6999, 0.4111, 0.1902, -0.4888 };
    std::vector<std::string>    strvec2 =
        { "4% of something", "Description 4/5", "This is bad",
          "3.4% of GDP", "Market drops", "Market pulls back",
          "$15 increase", "Running fast", "C++14 development",
          "Some explanation", "More strings", "Bonds vs. Equities",
          "Almost done", "Here comes the sun", "XXXX1", "XXXX04",
          "XXXX2", "XXXX3", "XXXX4", "XXXX4", "XXXX5", "XXXX6",
          "XXXX7", "XXXX10", "XXXX11", "XXXX01", "XXXX02", "XXXX03" };

    MyDataFrame dfx;

    dfx.load_data(std::move(ulgvec2),
                  std::make_pair("xint_col", intvec2),
                  std::make_pair("dbl_col", xdblvec2),
                  std::make_pair("dbl_col_2", dblvec22),
                  std::make_pair("str_col", strvec2),
                  std::make_pair("ul_col", xulgvec2));
    dfx.write<std::ostream,
              int,
              unsigned long,
              double,
              std::string>(std::cout);

    const MyDataFrame   dfxx =
        dfx.groupby<GroupbySum,
                    unsigned long,
                    int,
                    unsigned long,
                    std::string,
                    double>(GroupbySum());

    dfxx.write<std::ostream,
               int,
               unsigned long,
               double,
               std::string>(std::cout);

    const MyDataFrame   dfxx2 =
        dfx.groupby<GroupbySum,
                    std::string,
                    int,
                    unsigned long,
                    std::string,
                    double>(GroupbySum(), "str_col");

    dfxx2.write<std::ostream,
                int,
                unsigned long,
                double,
                std::string>(std::cout);

    std::future<MyDataFrame>    gb_fut =
        dfx.groupby_async<GroupbySum,
                          double,
                          int,
                          unsigned long,
                          std::string,
                          double>(GroupbySum(), "dbl_col_2");
    const MyDataFrame           dfxx3 = gb_fut.get();

    dfxx3.write<std::ostream,
                int,
                unsigned long,
                double,
                std::string>(std::cout);

    std::cout << "\nTesting Async write\n" << std::endl;

    std::future<bool>   fut =
        dfxx3.write_async<std::ostream,
                          int,
                          unsigned long,
                          double,
                          std::string>(std::cout);

    fut.get();

    std::cout << "\nTesting Bucketize()\n" << std::endl;

    std::future<void>   sort_fut =
        dfx.sort_async<double, int, double, std::string>();

    sort_fut.get();
    dfx.write<std::ostream,
              int,
              unsigned long,
              double,
              std::string>(std::cout);

    const MyDataFrame::IndexType    interval = 4;
    std::future<MyDataFrame>        b_fut =
        dfx.bucketize_async<GroupbySum,
                            int,
                            unsigned long,
                            std::string,
                            double>(GroupbySum(), interval);
    const MyDataFrame               buck_df = b_fut.get();

    buck_df.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  std::string>(std::cout, true);

    {
        std::cout << "\nTesting read()\n" << std::endl;

        MyDataFrame         df_read;
        // std::future<bool>   fut2 =
        //     df_read.read_async("../test/sample_data.csv");
        std::future<bool>   fut2 = df_read.read_async("sample_data.csv");

        fut2.get();
        df_read.write<std::ostream,
                      int,
                      unsigned long,
                      double,
                      std::string,
                      bool>(std::cout);

        StdDataFrame<std::string>   df_read_str;

        df_read_str.read_async("sample_data_string_index.csv");
        df_read_str.write<std::ostream,
                          int,
                          unsigned long,
                          double,
                          std::string,
                          bool>(std::cout);

        StdDataFrame<DateTime>  df_read_dt;

        df_read_dt.read_async("sample_data_dt_index.csv");
        df_read_dt.write<std::ostream,
                         int,
                         unsigned long,
                         double,
                         std::string,
                         bool>(std::cout);
    }

    std::cout << "\nTesting multi_visit()\n" << std::endl;

    MeanVisitor<int>            ivisitor2;
    MeanVisitor<unsigned long>  ulvisitor;
    MeanVisitor<double>         dvisitor2;
    MeanVisitor<double>         dvisitor22;

    dfx.multi_visit(std::make_pair("xint_col", &ivisitor2),
                    std::make_pair("dbl_col", &dvisitor2),
                    std::make_pair("dbl_col_2", &dvisitor22),
                    std::make_pair("ul_col", &ulvisitor));

    assert(ivisitor2.get_result() == 19);
    assert(abs(dvisitor2.get_result() - 4.5696) < 0.0001);
    assert(abs(dvisitor22.get_result() - 0.0264609) < 0.00001);
    assert(ulvisitor.get_result() == 123448);

    MyDataFrame df_copy_con = dfx;

    assert((df_copy_con.is_equal<int,
                                 unsigned long,
                                 double,
                                 std::string>(dfx)));
    assert((! df_copy_con.is_equal<int,
                                   unsigned long,
                                   double,
                                   std::string>(dfxx)));

    df_copy_con.get_column<double>("dbl_col")[7] = 88.888888;
    assert(dfx.get_column<double>("dbl_col")[7] == 10.0);
    assert(
       abs(df_copy_con.get_column<double>("dbl_col")[7] - 88.888888) < 0.00001);
    assert(! (df_copy_con.is_equal<int,
                                   unsigned long,
                                   double,
                                   std::string>(dfx)));

    std::cout << "dfx before modify_by_idx()" << std::endl;
    dfx.write<std::ostream,
              int,
              unsigned long,
              double,
              std::string>(std::cout);

    dfx.modify_by_idx<int, unsigned long, double, std::string>(df_copy_con);
    std::cout << "dfx after modify_by_idx()" << std::endl;
    dfx.write<std::ostream,
              int,
              unsigned long,
              double,
              std::string>(std::cout);
    dfx.modify_by_idx<int, unsigned long, double, std::string>(df);
    std::cout << "dfx after modify_by_idx()" << std::endl;
    dfx.write<std::ostream,
              int,
              unsigned long,
              double,
              std::string>(std::cout);

    MyDataFrame::set_thread_level(5);
    assert(MyDataFrame::get_thread_level() == 5);
    MyDataFrame::set_thread_level(0);
    assert(MyDataFrame::get_thread_level() == 0);
    MyDataFrame::set_thread_level(10);

    {
        std::cout << "\n\nTesting transpose()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123450, 123455, 123450, 123449 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14 };
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21 };
        std::vector<double> d4 = { 22, 23, 24, 25 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", d4));

        std::vector<unsigned long>  tidx = { 100, 101, 102, 104 };
        std::vector<const char *>   tcol_names =
            { "tcol_1", "tcol_2", "tcol_3",
              "tcol_4", "tcol_5", "tcol_6", "tcol_7" };
        MyDataFrame                 tdf =
            df.transpose<double>(std::move(tidx),
                                 { "col_1", "col_2", "col_3", "col_4" },
                                 tcol_names);

        std::cout << "Original DataFrame:" << std::endl;
        df.write<std::ostream, unsigned long, double>(std::cout);
        std::cout << "Transposed DataFrame:" << std::endl;
        tdf.write<std::ostream, unsigned long, double>(std::cout);
    }

    {
        std::cout << "\n\nTesting get_data_by_loc()/slicing" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123450, 123455, 123450, 123449 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14 };
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21 };
        std::vector<double> d4 = { 22, 23, 24, 25 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", d4));

        MyDataFrame df2 = df.get_data_by_loc<double>({ 3, 6 });
        MyDataFrame df3 = df.get_data_by_loc<double>({ 0, 7 });
        MyDataFrame df4 = df.get_data_by_loc<double>({ -4, -1 });
        MyDataFrame df5 = df.get_data_by_loc<double>({ -4, 6 });

        df.write<std::ostream, double>(std::cout);
        df2.write<std::ostream, double>(std::cout);
        df3.write<std::ostream, double>(std::cout);
        df4.write<std::ostream, double>(std::cout);
        df5.write<std::ostream, double>(std::cout);

        try  {
            MyDataFrame df2 = df.get_data_by_loc<double>({ 3, 8 });
        }
        catch (const BadRange &ex)  {
            std::cout << "Caught: " << ex.what() << std::endl;
        }
        try  {
            MyDataFrame df2 = df.get_data_by_loc<double>({ -8, -1 });
        }
        catch (const BadRange &ex)  {
            std::cout << "Caught: " << ex.what() << std::endl;
        }
    }

    {
        std::cout << "\n\nTesting get_view_by_loc()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123450, 123455, 123450, 123449 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14 };
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21 };
        std::vector<double> d4 = { 22, 23, 24, 25 };
        std::vector<std::string> s1 =
            { "11", "22", "33", "xx", "yy", "gg", "string" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", d4),
                     std::make_pair("col_str", s1));

        typedef DataFrameView<unsigned long> MyDataFrameView;

        MyDataFrameView dfv = df.get_view_by_loc<double, std::string>({ 3, 6 });

        dfv.write<std::ostream, double, std::string>(std::cout);
        dfv.get_column<double>("col_3")[0] = 88.0;
        assert(dfv.get_column<double>("col_3")[0] ==
               df.get_column<double>("col_3")[3]);
        assert(dfv.get_column<double>("col_3")[0] == 88.0);
    }

    {
        std::cout << "\n\nTesting remove_column()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123450, 123455, 123450, 123449 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14 };
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21 };
        std::vector<int>    i1 = { 22, 23, 24, 25 };
        std::vector<std::string> s1 =
            { "11", "22", "33", "xx", "yy", "gg", "string" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_int", i1),
                     std::make_pair("col_str", s1));


        df.write<std::ostream, double, int, std::string>(std::cout);
        df.remove_column("col_2");
        std::cout << "After removing column `col_2`" << std::endl;
        df.write<std::ostream, double, int, std::string>(std::cout);
        df.remove_column("col_str");
        std::cout << "After removing column `col_str`" << std::endl;
        df.write<std::ostream, double, int, std::string>(std::cout);

        std::vector<double> d22 = { 8, 9, 10, 11, 12, 13, 14 };

        df.load_column("col_2", std::move(d22));
        std::cout << "After adding back column `col_2`" << std::endl;
        df.write<std::ostream, double, int, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting get_view_by_idx()/slicing" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.1, 0.89, 0.45 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99, 100, 101, 3, 2 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        typedef DataFrameView<unsigned long> MyDataFrameView;

        MyDataFrame     df2 =
            df.get_data_by_idx<double, int>({ 123452, 123460 });
        MyDataFrameView dfv =
            df.get_view_by_idx<double, int>({ 123452, 123460 });

        df.write<std::ostream, double, int>(std::cout);
        df2.write<std::ostream, double, int>(std::cout);
        dfv.write<std::ostream, double, int>(std::cout);

        dfv.get_column<double>("col_3")[0] = 88.0;
        assert(dfv.get_column<double>("col_3")[0] ==
               df.get_column<double>("col_3")[2]);
        assert(dfv.get_column<double>("col_3")[0] == 88.0);
    }

    {
        std::cout << "\n\nTesting rename_column()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.1, 0.89, 0.45 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99, 100, 101, 3, 2 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::cout << "Before rename" << std::endl;
        df.write<std::ostream, double, int>(std::cout);
        df.rename_column("col_2", "renamed_column");
        std::cout << "After rename" << std::endl;
        df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting get_col_unique_values()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 0.89, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99, 100, 101, 3, 2 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        df.write<std::ostream, double, int>(std::cout);
        std::cout << "Getting unique values in column col_3" << std::endl;

        const std::vector<double>   result =
            df.get_col_unique_values<double>("col_3");

        for (auto &iter : result)
            std::cout << iter << ",";
        std::cout << std::endl;
    }

    {
        std::cout << "\n\nTesting remove_data_by_idx()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.1, 0.89, 0.45 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99, 100, 101, 3, 2 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        df.write<std::ostream, double, int>(std::cout);
        std::cout << "After removing by ibdex { 123452, 123460 }" << std::endl;
        df.remove_data_by_idx<double, int>({ 123452, 123460 });
        df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting remove_data_by_loc()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.1, 0.89, 0.45 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        df.write<std::ostream, double, int>(std::cout);
        std::cout << "After removing by ibdex { 3, -3 }" << std::endl;
        df.remove_data_by_loc<double, int>({ 3, -3 });
        df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting value_counts()" << std::endl;

        const double                my_nan = sqrt(-1);
        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21, my_nan,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        df.write<std::ostream, double, int>(std::cout);

        auto    result = df.value_counts<double>("col_3");

        std::cout << "After calling value_counts(cols_3)" << std::endl;
        result.write<std::ostream, size_t>(std::cout);
    }

    {
        std::cout << "\n\nTesting Index Inner Join" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<unsigned long>  idx2 =
            { 123452, 123453, 123455, 123458, 123466, 223450, 223451,
              223454, 223456, 223457, 223459, 223460, 223461, 223462 };
        std::vector<double> d12 =
               { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
        std::vector<double> d22 =
            { 18, 19, 110, 111, 112, 113, 114, 120, 122, 123,
              130, 131, 132, 11.89 };
        std::vector<double> d32 = { 115, 116, 115, 118, 119, 116, 121,
                                    10.34, 11.56, 10.34, 12.3, 10.34, 119.0 };
        std::vector<int>    i12 = { 122, 123, 124, 125, 199 };
        MyDataFrame         df2;

        df2.load_data(std::move(idx2),
                      std::make_pair("xcol_1", d12),
                      std::make_pair("col_2", d22),
                      std::make_pair("xcol_3", d32),
                      std::make_pair("col_4", i12));

        std::cout << "First DF:" << std::endl;
        df.write<std::ostream, double, int>(std::cout);
        std::cout << "Second DF2:" << std::endl;
        df2.write<std::ostream, double, int>(std::cout);

        MyDataFrame join_df =
            df.join_by_index<decltype(df2), double, int>
                (df2, join_policy::inner_join);

        std::cout << "Now The joined DF:" << std::endl;
        join_df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting Index Left Join" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<unsigned long>  idx2 =
            { 123452, 123453, 123455, 123458, 123466, 223450, 223451,
              223454, 223456, 223457, 223459, 223460, 223461, 223462 };
        std::vector<double> d12 =
               { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
        std::vector<double> d22 =
            { 18, 19, 110, 111, 112, 113, 114, 120, 122, 123,
              130, 131, 132, 11.89 };
        std::vector<double> d32 = { 115, 116, 115, 118, 119, 116, 121,
                                    10.34, 11.56, 10.34, 12.3, 10.34, 119.0 };
        std::vector<int>    i12 = { 122, 123, 124, 125, 199 };
        MyDataFrame         df2;

        df2.load_data(std::move(idx2),
                      std::make_pair("xcol_1", d12),
                      std::make_pair("col_2", d22),
                      std::make_pair("xcol_3", d32),
                      std::make_pair("col_4", i12));

        std::cout << "First DF:" << std::endl;
        df.write<std::ostream, double, int>(std::cout);
        std::cout << "Second DF2:" << std::endl;
        df2.write<std::ostream, double, int>(std::cout);

        MyDataFrame join_df =
            df.join_by_index<decltype(df2), double, int>
                (df2, join_policy::left_join);

        std::cout << "Now The joined DF:" << std::endl;
        join_df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting Index Right Join" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<unsigned long>  idx2 =
            { 123452, 123453, 123455, 123458, 123466, 223450, 223451,
              223454, 223456, 223457, 223459, 223460, 223461, 223462 };
        std::vector<double> d12 =
               { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
        std::vector<double> d22 =
            { 18, 19, 110, 111, 112, 113, 114, 120, 122, 123,
              130, 131, 132, 11.89 };
        std::vector<double> d32 = { 115, 116, 115, 118, 119, 116, 121,
                                    10.34, 11.56, 10.34, 12.3, 10.34, 119.0 };
        std::vector<int>    i12 = { 122, 123, 124, 125, 199 };
        MyDataFrame         df2;

        df2.load_data(std::move(idx2),
                      std::make_pair("xcol_1", d12),
                      std::make_pair("col_2", d22),
                      std::make_pair("xcol_3", d32),
                      std::make_pair("col_4", i12));

        std::cout << "First DF:" << std::endl;
        df.write<std::ostream, double, int>(std::cout);
        std::cout << "Second DF2:" << std::endl;
        df2.write<std::ostream, double, int>(std::cout);

        MyDataFrame join_df =
            df.join_by_index<decltype(df2), double, int>
                (df2, join_policy::right_join);

        std::cout << "Now The joined DF:" << std::endl;
        join_df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting Index Left Right Join" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<unsigned long>  idx2 =
            { 123452, 123453, 123455, 123458, 123466, 223450, 223451,
              223454, 223456, 223457, 223459, 223460, 223461, 223462 };
        std::vector<double> d12 =
               { 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114 };
        std::vector<double> d22 =
            { 18, 19, 110, 111, 112, 113, 114, 120, 122, 123,
              130, 131, 132, 11.89 };
        std::vector<double> d32 = { 115, 116, 115, 118, 119, 116, 121,
                                    10.34, 11.56, 10.34, 12.3, 10.34, 119.0 };
        std::vector<int>    i12 = { 122, 123, 124, 125, 199 };
        MyDataFrame         df2;

        df2.load_data(std::move(idx2),
                      std::make_pair("xcol_1", d12),
                      std::make_pair("col_2", d22),
                      std::make_pair("xcol_3", d32),
                      std::make_pair("col_4", i12));

        std::cout << "First DF:" << std::endl;
        df.write<std::ostream, double, int>(std::cout);
        std::cout << "Second DF2:" << std::endl;
        df2.write<std::ostream, double, int>(std::cout);

        MyDataFrame join_df =
            df.join_by_index<decltype(df2), double, int>
                (df2, join_policy::left_right_join);

        std::cout << "Now The joined DF:" << std::endl;
        join_df.write<std::ostream, double, int>(std::cout);
    }

    {
        std::cout << "\n\nTesting Largest/Smallest visitors" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                   13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14, 20, 22, 23,
                                   30, 31, 32, 1.89};
        std::vector<double> d3 = { 15, 16, 15, 18, 19, 16, 21,
                                   0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>    i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, double, int>(std::cout);

        NLargestVisitor<5, double> nl_visitor;

        df.visit<double>("col_3", nl_visitor);
        std::cout << "N largest result for col_3:" << std::endl;
        for (auto iter : nl_visitor.get_result())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        nl_visitor.sort_by_index();
        std::cout << "N largest result for col_3 sorted by index:" << std::endl;
        for (auto iter : nl_visitor.get_result())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        nl_visitor.sort_by_value();
        std::cout << "N largest result for col_3 sorted by value:" << std::endl;
        for (auto iter : nl_visitor.get_result())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;

        NSmallestVisitor<5, double> ns_visitor;

        df.visit<double>("col_3", ns_visitor);
        std::cout << "N smallest result for col_3:" << std::endl;
        for (auto iter : ns_visitor.get_result())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        ns_visitor.sort_by_index();
        std::cout << "N smallest result for col_3 sorted by index:"
                  << std::endl;
        for (auto iter : ns_visitor.get_result())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        ns_visitor.sort_by_value();
        std::cout << "N smallest result for col_3 sorted by value:"
                  << std::endl;
        for (auto iter : ns_visitor.get_result())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
    }

    {
        std::cout << "\n\nTesting Shifting Up/Down" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double>         d1 = { 15, 16, 15, 18, 19, 16, 21,
                                           0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy", "uuuu",
              "iiii", "oooo", "pppp", "2222", "aaaa", "dddd", "ffff" };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dbl_col", d1),
                     std::make_pair("int_col", i1),
                     std::make_pair("str_col", s1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, double, int, std::string>(std::cout);

        auto    sudf = df.shift<double, int, std::string>(3, shift_policy::up);

        std::cout << "Shifted Up DF:" << std::endl;
        sudf.write<std::ostream, double, int, std::string>(std::cout);

        auto    sddf =
            df.shift<double, int, std::string>(3, shift_policy::down);

        std::cout << "Shifted Down DF:" << std::endl;
        sddf.write<std::ostream, double, int, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting Rotating Up/Down" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double>         d1 = { 15, 16, 15, 18, 19, 16, 21,
                                           0.34, 1.56, 0.34, 2.3, 0.34, 19.0 };
        std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy", "uuuu",
              "iiii", "oooo", "pppp", "2222", "aaaa", "dddd", "ffff" };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dbl_col", d1),
                     std::make_pair("int_col", i1),
                     std::make_pair("str_col", s1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, double, int, std::string>(std::cout);

        auto    rudf = df.rotate<double, int, std::string>(3, shift_policy::up);

        std::cout << "Rotated Up DF:" << std::endl;
        rudf.write<std::ostream, double, int, std::string>(std::cout);

        auto    rddf =
            df.rotate<double, int, std::string>(3, shift_policy::down);

        std::cout << "Rotated Down DF:" << std::endl;
        rddf.write<std::ostream, double, int, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting DataFrame with DateTime" << std::endl;

        DateTime                    dt(20010102);
        std::vector<DateTime>       idx;
        std::vector<double>         d1;
        std::vector<int>            i1;
        std::vector<std::string>    s1;

        idx.reserve(20);
        d1.reserve(20);
        i1.reserve(20);
        s1.reserve(20);
        for (int i = 0; i < 20; ++i)  {
            idx.emplace_back(dt);
            d1.push_back(i + 0.345689);
            i1.push_back(i);
            dt.add_days(1);
            s1.emplace_back("Test str");
        }

        StdDataFrame<DateTime>  df;

        df.load_data(std::move(idx),
                     std::make_pair("dbl_col", d1),
                     std::make_pair("int_col", i1),
                     std::make_pair("str_col", s1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, double, int, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting DataFrame friend plus operator" << std::endl;

        MyDataFrame df1;
        MyDataFrame df2;

        df1.read("sample_data.csv");
        df2.read("sample_data.csv");

        MyDataFrame result =
            df_plus<MyDataFrame,
                    unsigned long,
                    int,
                    double,
                    std::string,
                    bool>(df1, df2);

        std::cout << "Original DF1:" << std::endl;
        df1.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  std::string,
                  bool>(std::cout);
        std::cout << "Original DF2:" << std::endl;
        df2.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  std::string,
                  bool>(std::cout);
        std::cout << "Result DF:" << std::endl;
        result.write<std::ostream,
                     int,
                     unsigned long,
                     double,
                     std::string,
                     bool>(std::cout);
    }

    {
        std::cout << "\n\nTesting DataFrame friend minus operator" << std::endl;

        MyDataFrame df1;
        MyDataFrame df2;

        df1.read("sample_data.csv");
        df2.read("sample_data.csv");

        // Notice I am omitting std::string here, since minus is not defined for
        // std::string, and hence it won't compile
        MyDataFrame result =
            df_minus<MyDataFrame,
                     unsigned long,
                     int,
                     double,
                     bool>(df1, df2);

        std::cout << "Original DF1:" << std::endl;
        df1.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  std::string,
                  bool>(std::cout);
        std::cout << "Original DF2:" << std::endl;
        df2.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  std::string,
                  bool>(std::cout);
        std::cout << "Result DF:" << std::endl;
        result.write<std::ostream,
                     int,
                     unsigned long,
                     double,
                     bool>(std::cout);
    }

    {
        std::cout << "\n\nTesting DataFrame friend multiplis operator"
                  << std::endl;

        std::vector<unsigned long>  idx1 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 25, 40, 55 };
        std::vector<unsigned long>  idx2 =
            { 1, 2, 3, 4, 5, 8, 9, 22, 25, 40 };
        std::vector<double>         d1 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        std::vector<double>         d2 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        std::vector<double>         s1 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        std::vector<int>            s2 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        MyDataFrame df1;
        MyDataFrame df2;

        df1.load_data(std::move(idx1),
                      std::make_pair("dbl_col", d1),
                      std::make_pair("same_name", s1));
        df2.load_data(std::move(idx2),
                      std::make_pair("dbl_col", d2),
                      std::make_pair("same_name", s2));

        MyDataFrame result =
            df_multiplies<MyDataFrame, int, double>(df1, df2);

        std::cout << "Original DF1:" << std::endl;
        df1.write<std::ostream, int, double>(std::cout);
        std::cout << "Original DF2:" << std::endl;
        df2.write<std::ostream, int, double>(std::cout);
        std::cout << "Result DF:" << std::endl;
        result.write<std::ostream, int, double>(std::cout);
    }

    {
        std::cout << "\n\nTesting DataFrame friend divides operator"
                  << std::endl;

        std::vector<unsigned long>  idx1 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 25, 40, 55 };
        std::vector<unsigned long>  idx2 =
            { 1, 2, 3, 4, 5, 8, 9, 22, 25, 40 };
        std::vector<double>         d1 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        std::vector<double>         d2 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        std::vector<double>         s1 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        std::vector<int>            s2 =
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        MyDataFrame df1;
        MyDataFrame df2;

        df1.load_data(std::move(idx1),
                      std::make_pair("dbl_col", d1),
                      std::make_pair("same_name", s1));
        df2.load_data(std::move(idx2),
                      std::make_pair("dbl_col", d2),
                      std::make_pair("same_name", s2));

        MyDataFrame result = df_divides<MyDataFrame, int, double>(df1, df2);

        std::cout << "Original DF1:" << std::endl;
        df1.write<std::ostream, int, double>(std::cout);
        std::cout << "Original DF2:" << std::endl;
        df2.write<std::ostream, int, double>(std::cout);
        std::cout << "Result DF:" << std::endl;
        result.write<std::ostream, int, double>(std::cout);
    }

    {
        std::cout << "\n\nTesting fill_missing(values)" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy", "iiii", "oooo" };

        df.load_column("col_str",
                       std::move(s1),
                       nan_policy::dont_pad_with_nans);

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.fill_missing<double, 3>({ "col_1", "col_2", "col_3" },
                                   fill_policy::value,
                                   { 1001, 1002, 1003 },
                                   3);
        df.fill_missing<std::string, 1>({ "col_str" },
                                        fill_policy::value,
                                        { "XXXXXX" });

        std::cout << "After fill missing with values DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting fill_missing(fill_forward)" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy", "iiii", "oooo" };

        df.load_column("col_str",
                       std::move(s1),
                       nan_policy::dont_pad_with_nans);

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.fill_missing<double, 3>({ "col_1", "col_2", "col_3" },
                                   fill_policy::fill_forward,
                                   { },
                                   3);
        df.fill_missing<std::string, 1>({ "col_str" },
                                        fill_policy::fill_forward,
                                        {  }, 3);

        std::cout << "After fill missing with values DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting fill_missing(fill_backward)" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy",
              "iiii", "oooo", "pppp", "2222", "aaaa", "dddd" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_str", s1),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.fill_missing<double, 3>({ "col_1", "col_2", "col_3" },
                                   fill_policy::fill_backward);
        df.fill_missing<std::string, 1>({ "col_str" },
                                        fill_policy::fill_backward);

        std::cout << "After fill missing with values DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting fill_missing(linear_interpolate)" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double>(std::cout);

        df.fill_missing<double, 3>({ "col_1", "col_2", "col_3" },
                                   fill_policy::linear_interpolate);

        std::cout << "After fill missing with values DF:" << std::endl;
        df.write<std::ostream, int, double>(std::cout);
    }

    {
        std::cout << "\n\nTesting drop_missing(all) no drop" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy",
              "iiii", "oooo", "pppp", "2222", "aaaa", "dddd" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_str", s1),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.drop_missing<int, double, std::string>(drop_policy::all);

        std::cout << "After drop missing all DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting drop_missing(all) 2 drop" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "",
              "iiii", "oooo", "pppp", "2222", "", "dddd" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_str", s1),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.drop_missing<int, double, std::string>(drop_policy::all);

        std::cout << "After drop missing all DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting drop_missing(any)" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy",
              "iiii", "oooo", "pppp", "2222", "aaaa", "dddd" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_str", s1),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.drop_missing<int, double, std::string>(drop_policy::any);

        std::cout << "After drop missing all DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting drop_missing(threshold=3)" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { std::numeric_limits<double>::quiet_NaN(),
                                   16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22,
                                   std::numeric_limits<int>::quiet_NaN(),
                                   std::numeric_limits<int>::quiet_NaN(),
                                   25,
                                   std::numeric_limits<int>::quiet_NaN() };
        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy",
              "iiii", "oooo", "pppp", "2222", "aaaa", "dddd" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_str", s1),
                     std::make_pair("col_4", i1));

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        df.drop_missing<int, double, std::string>(drop_policy::threshold, 3);

        std::cout << "After drop missing all DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);
    }

    {
        std::cout << "\n\nTesting get_row()" << std::endl;

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466 };
        std::vector<double> d1 = { 1, 2, 3, 4,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   6, 7,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   11, 12, 13, 14 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::quiet_NaN(),
                                   20, 22, 23, 30, 31,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   1.89 };
        std::vector<double> d3 = { 400.4, 16, 500.5, 18, 19, 16,
                                   std::numeric_limits<double>::quiet_NaN(),
                                   0.34, 1.56, 0.34, 2.3, 0.34,
                                   std::numeric_limits<double>::quiet_NaN() };
        std::vector<int>    i1 = { 22, 11, 34, 25,
                                   std::numeric_limits<int>::quiet_NaN() };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_4", i1));

        std::vector<std::string>    s1 =
            { "qqqq", "wwww", "eeee", "rrrr", "tttt", "yyyy", "iiii", "oooo" };

        df.load_column("col_str",
                       std::move(s1),
                       nan_policy::dont_pad_with_nans);

        std::vector<int>    i2 = { 22, 11 };

        df.load_column("col_int",
                       std::move(i2),
                       nan_policy::dont_pad_with_nans);

        std::cout << "Original DF:" << std::endl;
        df.write<std::ostream, int, double, std::string>(std::cout);

        std::array<const char *, 6> columns =
            {"col_1", "col_2", "col_3", "col_4", "col_str", "col_int"};
        auto                        row =
            df.get_row<6, int, double, std::string>(2, columns);

        assert(row.at<MyDataFrame::IndexType>(0) == 123452);
        assert(row.at<double>(0) == 3.0);
        assert(row.at<double>(1) == 10.0);
        assert(row.at<double>(2) == 500.5);
        assert(row.at<int>(0) == 34);
        assert(row.at<int>(1) == 0);
        assert(row.at<std::string>(0) == "eeee");
    }

    {
        // Testing Auto Correlation

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 15, 16, 15, 18, 19, 16, 21,
              0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 0.387,
              0.123, 1.06, 0.65, 2.03, 0.4, 1.0, 0.007 };
        std::vector<double>         d2 =
            { 1.23, 1.22, 1.21, 1.20, 1.19, 1.185, 1.181,
              1.19, 1.195, 1.189, 1.185, 1.18, 1.181, 1.186,
              1.189, 1.19, 1.194, 1.198, 1.199, 1.197, 1.193 };
        std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", i1));

        AutoCorrVisitor<double> auto_corr;
        const auto              &result =
            df.single_act_visit<double>("col_1", auto_corr).get_result();

        assert(result.size() == 17);
        assert(result[0] == 1.0);
        assert(abs(result[1] - 0.562001) < 0.00001);
        assert(abs(result[16] - -0.265228) < 0.00001);
        assert(abs(result[6] - 0.388131) < 0.00001);
        assert(abs(result[10] - 0.125514) < 0.00001);

        const auto  &result2 =
            df.single_act_visit<double>("col_2", auto_corr).get_result();

        assert(result.size() == 17);
        assert(result[0] == 1.0);
        assert(abs(result[1] - 0.903754) < 0.00001);
        assert(abs(result[16] - 0.183254) < 0.00001);
        assert(abs(result[6] - -0.263385) < 0.00001);
        assert(abs(result[10] - -0.712274) < 0.00001);
    }

    {
        // Testing Return

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 15, 16, 15, 18, 19, 16, 21,
              0.34, 1.56, 0.34, 2.3, 0.34, 19.0, 0.387,
              0.123, 1.06, 0.65, 2.03, 0.4, 1.0, 0.59 };
        std::vector<double>         d2 =
            { 1.23, 1.22, 1.21, 1.20, 1.19, 1.185, 1.181,
              1.19, 1.195, 1.189, 1.185, 1.18, 1.181, 1.186,
              1.189, 1.19, 1.194, 1.198, 1.199, 1.197, 1.193 };
        std::vector<int>            i1 = { 22, 23, 24, 25, 99 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", i1));

        ReturnVisitor<double>   return_visit(return_policy::monetary);
        const auto              &result =
            df.single_act_visit<double>("col_1", return_visit).get_result();

        assert(result.size() == 20);
        assert(result[0] == 1.0);
        assert(result[1] == -1.0);
        assert(result[16] == 1.38);
        assert(result[6] == -20.66);
        assert(abs(result[10] - -1.96) < 0.00001);

        ReturnVisitor<double>   return_visit2(return_policy::percentage);
        const auto              &result2 =
            df.single_act_visit<double>("col_1", return_visit2).get_result();

        assert(result2.size() == 20);
        assert(abs(result2[0] - 0.0666667) < 0.00001);
        assert(abs(result2[1] - -0.0625) < 0.00001);
        assert(abs(result2[16] - 2.12308) < 0.00001);
        assert(abs(result2[6] - -0.98381) < 0.00001);
        assert(abs(result2[10] - -0.852174) < 0.00001);

        ReturnVisitor<double>   return_visit3(return_policy::log);
        const auto              &result3 =
            df.single_act_visit<double>("col_1", return_visit3).get_result();

        assert(result3.size() == 20);
        assert(abs(result3[0] - 0.0645385) < 0.00001);
        assert(abs(result3[1] - -0.0645385) < 0.00001);
        assert(abs(result3[16] - 1.13882) < 0.00001);
        assert(abs(result3[6] - -4.12333) < 0.00001);
        assert(abs(result3[10] - -1.91172) < 0.00001);
    }

    {
        // Testing Median

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d2 =
            { 1.0, 10, 8, 18, 19, 16,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<int>           i1 =
            { 1, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<int>            i2 =
            { 1, 10, 8, 18, 19, 16,
              17, 20, 3, 2, 11, 7, 5,
              9, 15, 14, 13, 12, 6, 4 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("intcol_1", i1));
        df.load_column("dblcol_2",
                       std::move(d2),
                       nan_policy::dont_pad_with_nans);
        df.load_column("intcol_2",
                       std::move(i2),
                       nan_policy::dont_pad_with_nans);

        MedianVisitor<double>   med_visit;
        double                  result =
            df.single_act_visit<double>("dblcol_1", med_visit).get_result();

        assert(result == 10.0);

        result = df.single_act_visit<double>("dblcol_2",
                                             med_visit).get_result();
        assert(result == 10.50);

        MedianVisitor<int>  med_visit2;
        int                 result2 =
            df.single_act_visit<int>("intcol_1", med_visit2).get_result();

        assert(result2 == 10);

        result2 = df.single_act_visit<int>("intcol_2",
                                           med_visit2).get_result();
        assert(result2 == 10);
    }

    {
        // Testing Tracking Error

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d2 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d3 =
            { 1.1, 10.09, 8.2, 18.03, 19.4, 15.9, 20.8,
              17.1, 19.9, 3.3, 2.2, 10.8, 7.4, 5.3,
              9.1, 14.9, 14.8, 13.2, 12.6, 6.1, 4.4 };
        std::vector<double>         d4 =
            { 0.1, 9.09, 7.2, 17.03, 18.4, 14.9, 19.8,
              16.1, 18.9, 2.3, 1.2, 9.8, 6.4, 4.3,
              8.1, 13.9, 13.8, 12.2, 11.6, 5.1, 3.4 };
        std::vector<double>         d5 =
            { 20.0, 10.1, -30.2, 18.5, 1.1, 16.2, 30.8,
              -1.56, 20.1, 25.5, 30.89, 11.1, 7.4, 5.3,
              19, 15.1, 1.3, 1.2, 12.6, 23.2, 40.1 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("dblcol_2", d2),
                     std::make_pair("dblcol_3", d3),
                     std::make_pair("dblcol_4", d4),
                     std::make_pair("dblcol_5", d5));

        TrackingErrorVisitor<double>    tracking_visit;
        double                          result =
            df.visit<double, double>("dblcol_1",
                                     "dblcol_2",
                                     tracking_visit).get_result();

        assert(result == 0.0);

        result = df.visit<double, double>("dblcol_1",
                                          "dblcol_3",
                                          tracking_visit).get_result();
        assert(abs(result - 0.256416) < 0.00001);

        result = df.visit<double, double>("dblcol_1",
                                          "dblcol_4",
                                          tracking_visit).get_result();
        assert(abs(result - 0.256416) < 0.00001);

        result = df.visit<double, double>("dblcol_3",
                                          "dblcol_4",
                                          tracking_visit).get_result();
        assert(result == 0.0);

        result = df.visit<double, double>("dblcol_2",
                                          "dblcol_4",
                                          tracking_visit).get_result();
        assert(abs(result - 0.256416) < 0.00001);

        result = df.visit<double, double>("dblcol_1",
                                          "dblcol_5",
                                          tracking_visit).get_result();
        assert(abs(result - 17.0566) < 0.0001);
    }

    {
        // Testing Beta

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d2 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d3 =
            { 1.1, 10.09, 8.2, 18.03, 19.4, 15.9, 20.8,
              17.1, 19.9, 3.3, 2.2, 10.8, 7.4, 5.3,
              9.1, 14.9, 14.8, 13.2, 12.6, 6.1, 4.4 };
        std::vector<double>         d4 =
            { 0.1, 9.09, 7.2, 17.03, 18.4, 14.9, 19.8,
              16.1, 18.9, 2.3, 1.2, 9.8, 6.4, 4.3,
              8.1, 13.9, 13.8, 12.2, 11.6, 5.1, 3.4 };
        std::vector<double>         d5 =
            { 20.0, 10.1, -30.2, 18.5, 1.1, 16.2, 30.8,
              -1.56, 20.1, 25.5, 30.89, 11.1, 7.4, 5.3,
              19, 15.1, 1.3, 1.2, 12.6, 23.2, 40.1 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("dblcol_2", d2),
                     std::make_pair("dblcol_3", d3),
                     std::make_pair("dblcol_4", d4),
                     std::make_pair("dblcol_5", d5));

        ReturnVisitor<double>   return_visit(return_policy::log);

        df.load_column(
            "dblcol_1_return",
            df.single_act_visit<double>("dblcol_1",
                                        return_visit).get_result(),
            nan_policy::dont_pad_with_nans);
        df.load_column(
            "dblcol_2_return",
            df.single_act_visit<double>("dblcol_2",
                                        return_visit).get_result(),
            nan_policy::dont_pad_with_nans);
        df.load_column(
            "dblcol_3_return",
            df.single_act_visit<double>("dblcol_3",
                                        return_visit).get_result(),
            nan_policy::dont_pad_with_nans);
        df.load_column(
            "dblcol_4_return",
            df.single_act_visit<double>("dblcol_4",
                                        return_visit).get_result(),
            nan_policy::dont_pad_with_nans);
        df.load_column(
            "dblcol_5_return",
            df.single_act_visit<double>("dblcol_5",
                                        return_visit).get_result(),
            nan_policy::dont_pad_with_nans);

        BetaVisitor<double> beta_visit;
        double              result =
            df.visit<double, double>("dblcol_1_return",
                                     "dblcol_2_return",
                                     beta_visit).get_result();

        assert(result == 1.0);

        result = df.visit<double, double>("dblcol_1_return",
                                          "dblcol_3_return",
                                          beta_visit).get_result();
        assert(abs(result - 1.04881) < 0.00001);

        result = df.visit<double, double>("dblcol_1_return",
                                          "dblcol_4_return",
                                          beta_visit).get_result();
        assert(abs(result - 0.647582) < 0.00001);

        result = df.visit<double, double>("dblcol_1_return",
                                          "dblcol_5_return",
                                          beta_visit).get_result();
        assert(abs(result - -0.128854) < 0.00001);
    }

    {
        // Testing gen_datetime_index()

        std::vector<unsigned long>  idx_vec1 =
            MyDataFrame::gen_datetime_index("01/01/2018",
                                            "12/31/2038",
                                            time_frequency::annual);

        assert(idx_vec1.size() == 21);
        assert(idx_vec1.capacity() == 22);
        assert(idx_vec1[0] == 20180101);
        assert(idx_vec1[1] == 20190101);
        assert(idx_vec1[10] == 20280101);
        assert(idx_vec1[20] == 20380101);

        idx_vec1 = MyDataFrame::gen_datetime_index("01/01/2018",
                                                   "12/31/2038",
                                                   time_frequency::monthly,
                                                   3);
        assert(idx_vec1.size() == 84);
        assert(idx_vec1.capacity() == 86);
        assert(idx_vec1[0] == 20180101);
        assert(idx_vec1[1] == 20180401);
        assert(idx_vec1[2] == 20180701);
        assert(idx_vec1[40] == 20280101);
        assert(idx_vec1[83] == 20381001);

        idx_vec1 = MyDataFrame::gen_datetime_index("01/01/2018",
                                                   "12/31/2038",
                                                   time_frequency::weekly,
                                                   4);
        assert(idx_vec1.size() == 274);
        assert(idx_vec1.capacity() == 274);
        assert(idx_vec1[0] == 20180101);
        assert(idx_vec1[1] == 20180129);
        assert(idx_vec1[2] == 20180226);
        assert(idx_vec1[272] == 20381108);
        assert(idx_vec1[273] == 20381206);

        idx_vec1 = MyDataFrame::gen_datetime_index("01/01/2018",
                                                   "12/31/2038",
                                                   time_frequency::daily);
        assert(idx_vec1.size() == 7669);
        assert(idx_vec1.capacity() == 7670);
        assert(idx_vec1[0] == 20180101);
        assert(idx_vec1[1] == 20180102);
        assert(idx_vec1[2] == 20180103);
        assert(idx_vec1[7667] == 20381229);
        assert(idx_vec1[7668] == 20381230);

        idx_vec1 = MyDataFrame::gen_datetime_index("01/01/2018",
                                                   "12/31/2022",
                                                   time_frequency::hourly);
        assert(idx_vec1.size() == 43800);
        assert(idx_vec1.capacity() == 43801);
        assert(idx_vec1[0] == 1514782800);
        assert(idx_vec1[1] == 1514786400);
        assert(idx_vec1[2] == 1514790000);
        assert(idx_vec1[43798] == 1672455600);
        assert(idx_vec1[43799] == 1672459200);

        idx_vec1 = MyDataFrame::gen_datetime_index("01/01/2018",
                                                   "03/31/2018",
                                                   time_frequency::secondly,
                                                   10);
        assert(idx_vec1.size() == 768600);
        assert(idx_vec1.capacity() == 768601);
        assert(idx_vec1[0] == 1514782800);
        assert(idx_vec1[1] == 1514782810);
        assert(idx_vec1[2] == 1514782820);
        assert(idx_vec1[768598] == 1522468780);
        assert(idx_vec1[768599] == 1522468790);

        idx_vec1 = MyDataFrame::gen_datetime_index(
            "01/01/2018 00:00:00.000",
            "01/01/2018 10:10:01.600",
            time_frequency::millisecondly,
            500);
        assert(idx_vec1.size() == 73204);
        assert(idx_vec1.capacity() == 73229);
        assert(idx_vec1[0] == 1514782800000000000);
        assert(idx_vec1[1] == 1514782800500000000);
        assert(idx_vec1[2] == 1514782801000000000);
        assert(idx_vec1[73201] == 1514819400500000000);
        assert(idx_vec1[73202] == 1514819401000000000);
        assert(idx_vec1[73203] == 1514819401500000000);

        std::vector<DateTime>   idx_vec2 =
            StdDataFrame<DateTime>::gen_datetime_index("01/01/2018",
                                                       "12/31/2022",
                                                       time_frequency::hourly);

        assert(idx_vec2.size() == 43800);
        assert(idx_vec2[0].string_format (DT_FORMAT::DT_TM2) ==
                   "01/01/2018 00:00:00.000");
        assert(idx_vec2[1].string_format (DT_FORMAT::DT_TM2) ==
                   "01/01/2018 01:00:00.000");
        assert(idx_vec2[2].string_format (DT_FORMAT::DT_TM2) ==
                   "01/01/2018 02:00:00.000");
        assert(idx_vec2[43798].string_format (DT_FORMAT::DT_TM2) ==
                   "12/30/2022 22:00:00.000");
        assert(idx_vec2[43799].string_format (DT_FORMAT::DT_TM2) ==
                   "12/30/2022 23:00:00.000");
    }

    {
        // Testing replace(1)

        std::vector<double> d1 = { 1.0, 10, 8, 18, 19, 16, 21,
                                   17, 20, 3, 2, 11, 7.0, 5,
                                   9, 15, 14, 13, 12, 6, 4 };
        std::vector<double> d2 = { 1.0, 10, 8, 18, 19, 16, 21,
                                   17, 20, 3, 2, 11, 7.0, 5,
                                   9, 15, 14, 13, 12, 6, 4 };
        std::vector<double> d3 = { 1.1, 10.09, 8.2, 18.03, 19.4, 15.9, 20.8,
                                   17.1, 19.9, 3.3, 2.2, 10.8, 7.4, 5.3,
                                   9.1, 14.9, 14.8, 13.2, 12.6, 6.1, 4.4 };
        std::vector<double> d4 = { 0.1, 9.09, 7.2, 17.03, 18.4, 14.9, 19.8,
                                   16.1, 18.9, 2.3, 1.2, 9.8, 6.4, 4.3,
                                   8.1, 13.9, 13.8, 12.2, 11.6, 5.1, 3.4 };
        std::vector<double> d5 = { 20.0, 10.1, -30.2, 18.5, 1.1, 16.2, 30.8,
                                   -1.56, 20.1, 25.5, 30.89, 11.1, 7.4, 5.3,
                                   19, 15.1, 1.3, 1.2, 12.6, 23.2, 40.1 };
        MyDataFrame         df;

        df.load_data(MyDataFrame::gen_datetime_index(
                         "01/01/2018",
                         "01/22/2018",
                         time_frequency::daily),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("dblcol_2", d2),
                     std::make_pair("dblcol_3", d3),
                     std::make_pair("dblcol_4", d4),
                     std::make_pair("dblcol_5", d5));
        assert(df.get_column<double>("dblcol_1")[0] == 1.0);
        assert(df.get_column<double>("dblcol_1")[20] == 4.0);
        assert(df.get_column<double>("dblcol_1")[1] == 10.0);
        assert(df.get_column<double>("dblcol_1")[2] == 8.0);
        assert(df.get_column<double>("dblcol_1")[6] == 21.0);
        assert(df.get_column<double>("dblcol_1")[7] == 17.0);
        assert(df.get_column<double>("dblcol_1")[11] == 11.0);
        assert(df.get_column<double>("dblcol_1")[15] == 15.0);

        assert(df.get_column<double>("dblcol_5")[0] == 20.0);
        assert(df.get_column<double>("dblcol_5")[20] == 40.1);
        assert(df.get_column<double>("dblcol_5")[1] == 10.1);
        assert(df.get_column<double>("dblcol_5")[2] == -30.2);
        assert(df.get_column<double>("dblcol_5")[3] == 18.5);
        assert(df.get_column<double>("dblcol_5")[10] == 30.89);
        assert(df.get_column<double>("dblcol_5")[11] == 11.1);
        assert(df.get_column<double>("dblcol_5")[17] == 1.2);
        assert(df.get_column<double>("dblcol_5")[19] == 23.2);

        auto    result1 = df.replace_async<double, 3>(
            "dblcol_1", { 10.0, 21.0, 11.0 }, { 1000.0, 2100.0, 1100.0 });
        auto    idx_result = df.replace_index<3>(
            { 20180101, 20180102, 20180103 }, { 1000, 2100, 1100 });
        auto    result2 = df.replace_async<double, 6>(
            "dblcol_5",
            { -45.0, -100.0, -30.2, 30.89, 40.1, 1.2 },
            { 0.0, 0.0, 300.0, 210.0, 110.0, 1200.0 },
            3);

        auto    count = result1.get();

        assert(count == 3);
        assert(df.get_column<double>("dblcol_1")[0] == 1.0);
        assert(df.get_column<double>("dblcol_1")[20] == 4.0);
        assert(df.get_column<double>("dblcol_1")[1] == 1000.0);
        assert(df.get_column<double>("dblcol_1")[2] == 8.0);
        assert(df.get_column<double>("dblcol_1")[6] == 2100.0);
        assert(df.get_column<double>("dblcol_1")[7] == 17.0);
        assert(df.get_column<double>("dblcol_1")[11] == 1100.0);
        assert(df.get_column<double>("dblcol_1")[15] == 15.0);

        count = result2.get();
        assert(count == 3);
        assert(df.get_column<double>("dblcol_5")[0] == 20.0);
        assert(df.get_column<double>("dblcol_5")[20] == 110.0);
        assert(df.get_column<double>("dblcol_5")[1] == 10.1);
        assert(df.get_column<double>("dblcol_5")[2] == 300.0);
        assert(df.get_column<double>("dblcol_5")[3] == 18.5);
        assert(df.get_column<double>("dblcol_5")[10] == 210.0);
        assert(df.get_column<double>("dblcol_5")[11] == 11.1);
        assert(df.get_column<double>("dblcol_5")[17] == 1.2);
        assert(df.get_column<double>("dblcol_5")[19] == 23.2);
    }

    {
        // Testing replace(2)

        std::vector<double> d1 = { 1.0, 10, 8, 18, 19, 16, 21,
                                   17, 20, 3, 2, 11, 7.0, 5,
                                   9, 15, 14, 13, 12, 6, 4 };
        std::vector<double> d2 = { 1.0, 10, 8, 18, 19, 16, 21,
                                   17, 20, 3, 2, 11, 7.0, 5,
                                   9, 15, 14, 13, 12, 6, 4 };
        std::vector<double> d3 = { 1.1, 10.09, 8.2, 18.03, 19.4, 15.9, 20.8,
                                   17.1, 19.9, 3.3, 2.2, 10.8, 7.4, 5.3,
                                   9.1, 14.9, 14.8, 13.2, 12.6, 6.1, 4.4 };
        std::vector<double> d4 = { 0.1, 9.09, 7.2, 17.03, 18.4, 14.9, 19.8,
                                   16.1, 18.9, 2.3, 1.2, 9.8, 6.4, 4.3,
                                   8.1, 13.9, 13.8, 12.2, 11.6, 5.1, 3.4 };
        std::vector<double> d5 = { 20.0, 10.1, -30.2, 18.5, 1.1, 16.2, 30.8,
                                   -1.56, 20.1, 25.5, 30.89, 11.1, 7.4, 5.3,
                                   19, 15.1, 1.3, 1.2, 12.6, 23.2, 40.1 };
        MyDataFrame         df;

        df.load_data(MyDataFrame::gen_datetime_index(
                         "01/01/2018",
                         "01/22/2018",
                         time_frequency::daily),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("dblcol_2", d2),
                     std::make_pair("dblcol_3", d3),
                     std::make_pair("dblcol_4", d4),
                     std::make_pair("dblcol_5", d5));
        assert(df.get_column<double>("dblcol_1")[0] == 1.0);
        assert(df.get_column<double>("dblcol_1")[19] == 6.0);
        assert(df.get_column<double>("dblcol_1")[20] == 4.0);
        assert(df.get_column<double>("dblcol_1")[2] == 8.0);
        assert(df.get_column<double>("dblcol_1")[14] == 9.0);

        ReplaceFunctor  functor;
        auto            result =
            df.replace_async<double, ReplaceFunctor>("dblcol_1", functor);

        result.get();
        assert(functor.count == 3);
        assert(df.get_column<double>("dblcol_1")[0] == 1.0);
        assert(df.get_column<double>("dblcol_1")[19] == 6.0);
        assert(df.get_column<double>("dblcol_1")[20] == 4000.0);
        assert(df.get_column<double>("dblcol_1")[2] == 8000.0);
        assert(df.get_column<double>("dblcol_1")[14] == 9000.0);

        auto    seq_vec = MyDataFrame::gen_sequence_index(1, 200, 4);

        assert(seq_vec.size() == 50);
        assert(seq_vec[0] == 1);
        assert(seq_vec[2] == 9);
        assert(seq_vec[3] == 13);
        assert(seq_vec[49] == 197);
        assert(seq_vec[48] == 193);
    }

    {
        // Testing some visitors

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d2 =
            { 1.0, 10, 8, 18, 19, 16,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<int>           i1 =
            { 1, 1, 2, 4, 3, 4, 5,
              2, 1, 2, 2, 3, 4, 5,
              7, 1, 2, 3, 2, 6, 4 };
        std::vector<int>            i2 =
            { 1, 10, 8, 18, 19, 16,
              17, 20, 3, 2, 11, 7, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d3 =
            { 1, 10, std::numeric_limits<double>::quiet_NaN(), 18, 19, 16,
              17, 20, std::numeric_limits<double>::quiet_NaN(),
              2, 11, 7, std::numeric_limits<double>::quiet_NaN(), 5,
              9, 15, 14, 13, 12, 6 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("intcol_1", i1));
        df.load_column("dblcol_2",
                       std::move(d2),
                       nan_policy::dont_pad_with_nans);
        df.load_column("intcol_2",
                       std::move(i2),
                       nan_policy::dont_pad_with_nans);
        df.load_column("dblcol_3",
                       std::move(d3),
                       nan_policy::dont_pad_with_nans);

        SumVisitor<int>     sum_visit;
        ProdVisitor<int>    prod_visit;
        int                 sum_result =
            df.visit<int>("intcol_2", sum_visit).get_result();
        int                 prod_result =
            df.visit<int>("intcol_1", prod_visit).get_result();

        assert(sum_result == 210);
        assert(prod_result == 464486400);

        CumSumVisitor<double>       cum_sum_visit;
        const std::vector<double>   &cum_sum_result =
            df.single_act_visit<double>("dblcol_3",
                                        cum_sum_visit).get_result();

        assert(cum_sum_result.size() == 20);
        assert(cum_sum_result[0] == 1);
        assert(cum_sum_result[1] == 11);
        assert(cum_sum_result[19] == 195);
        assert(cum_sum_result[18] == 189);
        assert(std::isnan(cum_sum_result[2]));
        assert(std::isnan(cum_sum_result[8]));

        CumMaxVisitor<double>       cum_max_visit;
        const std::vector<double>   &cum_max_result =
            df.single_act_visit<double>("dblcol_3",
                                        cum_max_visit).get_result();

        assert(cum_max_result.size() == 20);
        assert(cum_max_result[0] == 1);
        assert(cum_max_result[1] == 10);
        assert(cum_max_result[19] == 20);
        assert(cum_max_result[18] == 20);
        assert(std::isnan(cum_max_result[2]));
        assert(std::isnan(cum_max_result[8]));
    }

    {
        // Testing Mode

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456,
              123457, 123458, 123459, 123460, 123461, 123462, 123466,
              123467, 123468, 123469, 123470, 123471, 123472, 123473 };
        std::vector<double>         d1 =
            { 1.0, 10, 8, 18, 19, 16, 21,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d2 =
            { 1.0, 10, 8, 18, 19, 16,
              17, 20, 3, 2, 11, 7.0, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<int>           i1 =
            { 1, 1, 2, 4, 3, 4, 5,
              2, 1, 2, 2, 3, 4, 5,
              7, 1, 2, 3, 2, 6, 4 };
        std::vector<int>            i2 =
            { 1, 10, 8, 18, 19, 16,
              17, 20, 3, 2, 11, 7, 5,
              9, 15, 14, 13, 12, 6, 4 };
        std::vector<double>         d3 =
            { 1, 10, std::numeric_limits<double>::quiet_NaN(), 18, 19, 16,
              17, 20, std::numeric_limits<double>::quiet_NaN(),
              2, 11, 7, std::numeric_limits<double>::quiet_NaN(), 5,
              9, 15, 14, 13, 12, 6 };
        MyDataFrame                 df;

        df.load_data(std::move(idx),
                     std::make_pair("dblcol_1", d1),
                     std::make_pair("intcol_1", i1));
        df.load_column("dblcol_2",
                       std::move(d2),
                       nan_policy::dont_pad_with_nans);
        df.load_column("intcol_2",
                       std::move(i2),
                       nan_policy::dont_pad_with_nans);
        df.load_column("dblcol_3",
                       std::move(d3),
                       nan_policy::dont_pad_with_nans);

        ModeVisitor<3, double>  mode_visit;
        const auto              &result =
            df.single_act_visit<double>("dblcol_3", mode_visit).get_result();

        assert(result.size() == 3);
        assert(result[0].indices.size() == 3);
        assert(result[0].value_indices_in_col.size() == 3);
        assert(std::isnan(result[0].value));
        assert(result[0].repeat_count() == 3);
        assert(result[0].indices[1] == 123458);
        assert(result[0].value_indices_in_col[2] == 12);
    }



    {
        // Testing get_data_by_sel()

        std::vector<unsigned long>  idx =
            { 123450, 123451, 123452, 123453, 123454, 123455, 123456 };
        std::vector<double> d1 = { 1, 2, 3, 4, 5, 6, 7 };
        std::vector<double> d2 = { 8, 9, 10, 11, 12, 13, 14 };
        std::vector<double> d3 = { 15, 16, 17, 18, 19, 20, 21 };
        std::vector<double> d4 = { 22, 23, 24, 25 };
        std::vector<std::string> s1 =
            { "11", "22", "33", "ee", "ff", "gg", "ll" };
        MyDataFrame         df;

        df.load_data(std::move(idx),
                     std::make_pair("col_1", d1),
                     std::make_pair("col_2", d2),
                     std::make_pair("col_3", d3),
                     std::make_pair("col_str", s1));
        df.load_column("col_4",
                       std::move(d4),
                       nan_policy::dont_pad_with_nans);

        auto    functor =
            [](const unsigned long &, const double &val)-> bool {
                return (val >= 5);
            };
        auto    result =
            df.get_data_by_sel<double, decltype(functor), double, std::string>
                ("col_1", functor);

        assert(result.get_index().size() == 3);
        assert(result.get_column<double>("col_1").size() == 3);
        assert(result.get_column<std::string>("col_str").size() == 3);
        assert(result.get_column<double>("col_4").size() == 3);
        assert(result.get_index()[0] == 123454);
        assert(result.get_index()[2] == 123456);
        assert(result.get_column<double>("col_2")[1] == 13);
        assert(std::isnan(result.get_column<double>("col_4")[2]));
        assert(result.get_column<std::string>("col_str")[1] == "gg");
        assert(result.get_column<std::string>("col_str")[2] == "ll");
        assert(result.get_column<double>("col_1")[1] == 6);
        assert(result.get_column<double>("col_1")[2] == 7);

        auto    functor2 =
            [](const unsigned long &,
               const double &val1,
               const double &val2,
               const std::string val3)-> bool {
                return (val1 >= 5 || val2 == 15 || val3 == "33");
            };
        auto    result2 =
            df.get_data_by_sel<double,
                               double,
                               std::string,
                               decltype(functor2),
                               double, std::string>
            ("col_1", "col_3", "col_str", functor2);

        assert(result2.get_index().size() == 5);
        assert(result2.get_column<double>("col_1").size() == 5);
        assert(result2.get_column<std::string>("col_str").size() == 5);
        assert(result2.get_column<double>("col_4").size() == 5);
        assert(result2.get_index()[0] == 123450);
        assert(result2.get_index()[2] == 123454);
        assert(result2.get_index()[4] == 123456);
        assert(result2.get_column<double>("col_2")[0] == 8);
        assert(result2.get_column<double>("col_2")[1] == 10);
        assert(result2.get_column<double>("col_2")[3] == 13);
        assert(result2.get_column<double>("col_4")[0] == 22);
        assert(result2.get_column<double>("col_4")[1] == 24);
        assert(std::isnan(result2.get_column<double>("col_4")[2]));
        assert(result2.get_column<std::string>("col_str")[0] == "11");
        assert(result2.get_column<std::string>("col_str")[1] == "33");
        assert(result2.get_column<std::string>("col_str")[2] == "ff");
        assert(result2.get_column<std::string>("col_str")[4] == "ll");
        assert(result2.get_column<double>("col_1")[0] == 1);
        assert(result2.get_column<double>("col_1")[1] == 3);
        assert(result2.get_column<double>("col_1")[2] == 5);
    }

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
