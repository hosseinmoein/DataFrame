#include <iostream>
#include <string>
#include <typeinfo>
#include <limits>
#include <cmath>
#include <cassert>

#include "../DataFrame/include/DataFrame/DataFrame.h"
#include "../DataFrame/include/DataFrame/DataFrameVisitors.h"
#include "../DataFrame/include/DataFrame/DateTime.h"
#include "../DataFrame/include/DataFrame/DataFrameOperators.h"

using namespace hmdf;

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

    hmdf::MeanVisitor<int>      ivisitor;
    hmdf::MeanVisitor<double>   dvisitor;

    assert(df.visit<int>("int_col", ivisitor).get_value() == 1);
    assert(std::isnan(df.visit<double>("dbl_col", dvisitor).get_value()));

    df.get_column<double>("dbl_col")[5] = 6.5;
    df.get_column<double>("dbl_col")[6] = 7.5;
    df.get_column<double>("dbl_col")[7] = 8.5;
    assert(::abs(df.visit<double>("dbl_col", dvisitor).get_value() -
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

    df.sort<MyDataFrame::TimeStamp, int, double, std::string>();
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

    hmdf::CorrVisitor<double>   corr_visitor;

    std::cout << "Correlation between dbl_col and dbl_col_2 is: "
              << df.visit<double, double>("dbl_col",
                                          "dbl_col_2",
                                          corr_visitor).get_value()
              << std::endl;

    hmdf::StatsVisitor<double>  stats_visitor;

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

    hmdf::SLRegressionVisitor<double>   slr_visitor;

    df.visit<double, double>("dbl_col", "dbl_col_2", slr_visitor);
    assert(slr_visitor.get_count() == 8);
    assert(abs(slr_visitor.get_slope() - -0.0561415) < 0.00001);
    assert(abs(slr_visitor.get_intercept() - 0.602674) < 0.00001);
    assert(abs(slr_visitor.get_corr() - -0.358381) < 0.00001);
    assert(abs(df.visit<double, double>("dbl_col", "dbl_col_2",
                                        corr_visitor).get_value() -
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

    const MyDataFrame::TimeStamp    interval = 4;
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

    hmdf::MeanVisitor<int>              ivisitor2;
    hmdf::MeanVisitor<unsigned long>    ulvisitor;
    hmdf::MeanVisitor<double>           dvisitor2;
    hmdf::MeanVisitor<double>           dvisitor22;

    dfx.multi_visit(std::make_pair("xint_col", &ivisitor2),
                    std::make_pair("dbl_col", &dvisitor2),
                    std::make_pair("dbl_col_2", &dvisitor22),
                    std::make_pair("ul_col", &ulvisitor));

    assert(ivisitor2.get_value() == 19);
    assert(abs(dvisitor2.get_value() - 4.5696) < 0.0001);
    assert(abs(dvisitor22.get_value() - 0.0264609) < 0.00001);
    assert(ulvisitor.get_value() == 123448);

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
        std::cout << "\n\nTesing transpose()" << std::endl;

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
        std::cout << "\n\nTesing get_data_by_loc()/slicing" << std::endl;

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
        std::cout << "\n\nTesing get_view_by_loc()" << std::endl;

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
        std::cout << "\n\nTesing remove_column()" << std::endl;

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
        std::cout << "\n\nTesing get_view_by_idx()/slicing" << std::endl;

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
        std::cout << "\n\nTesing rename_column()" << std::endl;

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
        std::cout << "\n\nTesing get_col_unique_values()" << std::endl;

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
        std::cout << "\n\nTesing remove_data_by_idx()" << std::endl;

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
        std::cout << "\n\nTesing remove_data_by_loc()" << std::endl;

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
        std::cout << "\n\nTesing value_counts()" << std::endl;

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
        std::cout << "\n\nTesing Index Inner Join" << std::endl;

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
        std::cout << "\n\nTesing Index Left Join" << std::endl;

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
        std::cout << "\n\nTesing Index Right Join" << std::endl;

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
        std::cout << "\n\nTesing Index Left Right Join" << std::endl;

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
        std::cout << "\n\nTesing Largest/Smallest visitors" << std::endl;

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
        for (auto iter : nl_visitor.get_values())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        nl_visitor.sort_by_index();
        std::cout << "N largest result for col_3 sorted by index:" << std::endl;
        for (auto iter : nl_visitor.get_values())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        nl_visitor.sort_by_value();
        std::cout << "N largest result for col_3 sorted by value:" << std::endl;
        for (auto iter : nl_visitor.get_values())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;

        NSmallestVisitor<5, double> ns_visitor;

        df.visit<double>("col_3", ns_visitor);
        std::cout << "N smallest result for col_3:" << std::endl;
        for (auto iter : ns_visitor.get_values())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        ns_visitor.sort_by_index();
        std::cout << "N smallest result for col_3 sorted by index:"
                  << std::endl;
        for (auto iter : ns_visitor.get_values())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
        ns_visitor.sort_by_value();
        std::cout << "N smallest result for col_3 sorted by value:"
                  << std::endl;
        for (auto iter : ns_visitor.get_values())
            std::cout << iter.index << '|' << iter.value << " ";
        std::cout << std::endl;
    }

    {
        std::cout << "\n\nTesing Shifting Up/Down" << std::endl;

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
        std::cout << "\n\nTesing Rotating Up/Down" << std::endl;

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
        std::cout << "\n\nTesing DataFrame with DateTime" << std::endl;

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
        std::cout << "\n\nTesing DataFrame friend plus operator" << std::endl;

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
        std::cout << "\n\nTesing DataFrame friend minus operator" << std::endl;

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
        std::cout << "\n\nTesing DataFrame friend multiplis operator"
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
        std::cout << "\n\nTesing DataFrame friend divides operator"
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
        std::cout << "\n\nTesing fill_missing(values)" << std::endl;

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
        std::cout << "\n\nTesing fill_missing(fill_forward)" << std::endl;

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
        std::cout << "\n\nTesing fill_missing(fill_backward)" << std::endl;

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
        std::cout << "\n\nTesing fill_missing(linear_interpolate)" << std::endl;

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
        std::cout << "\n\nTesing drop_missing(all) no drop" << std::endl;

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
        std::cout << "\n\nTesing drop_missing(all) 2 drop" << std::endl;

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
        std::cout << "\n\nTesing drop_missing(any)" << std::endl;

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
        std::cout << "\n\nTesing drop_missing(threshold=3)" << std::endl;

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
        std::cout << "\n\nTesing get_row()" << std::endl;

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

        assert(row.at<MyDataFrame::TimeStamp>(0) == 123452);
        assert(row.at<double>(0) == 3.0);
        assert(row.at<double>(1) == 10.0);
        assert(row.at<double>(2) == 500.5);
        assert(row.at<int>(0) == 34);
        assert(row.at<int>(1) == 0);
        assert(row.at<std::string>(0) == "eeee");
    }

    {
        // Tesing Auto Correlation

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

        hmdf::AutoCorrVisitor<double>   auto_corr;
        const auto                      &result =
            df.single_act_visit<double>("col_1", auto_corr).get_value();

        assert(result.size() == 17);
        assert(result[0] == 1.0);
        assert(abs(result[1] - 0.562001) < 0.00001);
        assert(abs(result[16] - -0.265228) < 0.00001);
        assert(abs(result[6] - 0.388131) < 0.00001);
        assert(abs(result[10] - 0.125514) < 0.00001);

        const auto  &result2 =
            df.single_act_visit<double>("col_2", auto_corr).get_value();

        assert(result.size() == 17);
        assert(result[0] == 1.0);
        assert(abs(result[1] - 0.903754) < 0.00001);
        assert(abs(result[16] - 0.183254) < 0.00001);
        assert(abs(result[6] - -0.263385) < 0.00001);
        assert(abs(result[10] - -0.712274) < 0.00001);
    }

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
