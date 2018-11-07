#include <iostream>
#include <string>
#include <typeinfo>
#include <cmath>

#include "../include/DataFrame.h"
#include "../include/DataFrameVisitors.h"

using namespace hmdf;

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    typedef StdDataFrame<unsigned long> MyDataFrame;

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

    int rc = df.load_data(std::move(ulgvec),
                          std::make_pair("int_col", intvec),
                          std::make_pair("dbl_col", dblvec),
                          std::make_pair("dbl_col_2", dblvec2),
                          std::make_pair("str_col", strvec),
                          std::make_pair("ul_col", xulgvec));

    std::cout << "Return code " << rc
              << " should be " <<
        ulgvec.size() +
        intvec.size() +
        dblvec.size() +
        dblvec2.size() +
        strvec.size() +
        ulgvec.size()
              << std::endl;

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

    std::cout << "Data is: " << df.get_column<double> ("dbl_col")[2]
              << std::endl;

    hmdf::MeanVisitor<int>      ivisitor;
    hmdf::MeanVisitor<double>   dvisitor;

    std::cout << "Integer average is: "
              << df.visit<int>("int_col", ivisitor).get_value()
              << std::endl;
    std::cout << "Double average is: "
              << df.visit<double>("dbl_col", dvisitor).get_value()
              << std::endl;

    df.get_column<double>("dbl_col")[5] = 6.5;
    df.get_column<double>("dbl_col")[6] = 7.5;
    df.get_column<double>("dbl_col")[7] = 8.5;
    dvisitor.reset();
    std::cout << "Double average is: "
              << df.visit<double>("dbl_col", dvisitor).get_value()
              << std::endl;

    df.write<std::ostream, int, unsigned long, double, std::string>(std::cout);

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
    std::cout << "Skewness of dbl_col is: "
              << stats_visitor.get_skew()
              << std::endl;
    std::cout << "Kurtosis of dbl_col is: "
              << stats_visitor.get_kurtosis()
              << std::endl;
    std::cout << "Mean of dbl_col is: "
              << stats_visitor.get_mean()
              << std::endl;
    std::cout << "Variamce of dbl_col is: "
              << stats_visitor.get_variance()
              << std::endl;

   std::cout <<"\nDoing simple linear regression between dbl_col and dbl_col_2"
             << std::endl;

    hmdf::SLRegressionVisitor<double>   slr_visitor;

    df.visit<double, double>("dbl_col", "dbl_col_2", slr_visitor);
    std::cout << "Count of dbl_col and dbl_col_2 is: "
              << slr_visitor.get_count() << std::endl;
    std::cout << "Slope of dbl_col and dbl_col_2 is: "
              << slr_visitor.get_slope() << std::endl;
    std::cout << "Intercept of dbl_col and dbl_col_2 is: "
              << slr_visitor.get_intercept() << std::endl;
    std::cout << "Correlation of dbl_col and dbl_col_2 is: "
              << slr_visitor.get_corr() << std::endl;
    corr_visitor.reset();
    std::cout << "Old correlation between dbl_col and dbl_col_2 is: "
              << df.visit<double, double>("dbl_col",
                                          "dbl_col_2",
                                          corr_visitor).get_value()
              << std::endl;

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
    dfx.write<std::ostream, int, unsigned long, double, std::string>(std::cout);

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

    std::cout << "\nTesting read()\n" << std::endl;


    MyDataFrame         df_read;
    std::future<bool>   fut2 = df_read.read_async("../test/sample_data.csv");

    fut2.get();
    df_read.write<std::ostream,
                  int,
                  unsigned long,
                  double,
                  std::string>(std::cout);

    std::cout << "\nTesting multi_visit()\n" << std::endl;

    hmdf::MeanVisitor<int>              ivisitor2;
    hmdf::MeanVisitor<unsigned long>    ulvisitor;
    hmdf::MeanVisitor<double>           dvisitor2;
    hmdf::MeanVisitor<double>           dvisitor22;

    dfx.multi_visit(std::make_pair("xint_col", &ivisitor2),
                    std::make_pair("dbl_col", &dvisitor2),
                    std::make_pair("dbl_col_2", &dvisitor22),
                    std::make_pair("ul_col", &ulvisitor));

    std::cout << "Integer average is: " << ivisitor2.get_value()
              << std::endl;
    std::cout << "Double average is: " << dvisitor2.get_value()
              << std::endl;
    std::cout << "Double2 average is: " << dvisitor22.get_value()
              << std::endl;
    std::cout << "ULong average is: " << ulvisitor.get_value()
              << std::endl;

    std::cout << "\nTesting constructors and assignments\n" << std::endl;

    MyDataFrame df_copy_con = dfx;

    std::cout << "These must be Equal: "
              << df_copy_con.is_equal<int,
                                      unsigned long,
                                      double,
                                      std::string>(dfx)
              << std::endl;
    std::cout << "These must Not be Equal: "
              << df_copy_con.is_equal<int,
                                      unsigned long,
                                      double,
                                      std::string>(dfxx)
              << std::endl;

    df_copy_con.get_column<double>("dbl_col")[7] = 88.888888;
    std::cout << "Values in dfx, df_copy_con: "
              << dfx.get_column<double>("dbl_col")[7] << ", "
              << df_copy_con.get_column<double>("dbl_col")[7]
              << std::endl;
    std::cout << "After the change, these must Not be Equal: "
              << df_copy_con.is_equal<int,
                                      unsigned long,
                                      double,
                                      std::string>(dfx)
              << std::endl;

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
        std::vector<std::string>    tcol_names =
            { "tcol_1", "tcol_2", "tcol_3",
              "tcol_4", "tcol_5", "tcol_6", "tcol_7" };
        MyDataFrame                 tdf =
            df.transpose<double>(std::move(tidx), tcol_names);

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
        std::cout << "After changing a value on view: "
                  << dfv.get_column<double>("col_3")[0]
                  << " == " << df.get_column<double>("col_3")[3]
                  << std::endl;
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
        std::cout << "After changing a value on view: "
                  << dfv.get_column<double>("col_3")[0]
                  << " == " << df.get_column<double>("col_3")[2]
                  << std::endl;
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

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
