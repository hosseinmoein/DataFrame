#include <DataFrame/DataFrame.h>

using namespace hmdf;

// A DataFrame with ulong index type
//
using MyDataFrameIdxType = unsigned long;
using MyDataFrame = StdDataFrame<MyDataFrameIdxType>;

struct MyDfSchema
{
    DECL_COL(col_1, double)
    DECL_COL(col_2, double)
    DECL_COL(col_3, double)
    DECL_COL(col_4, double)
    DECL_COL(col_str, std::string)
};

static void test_get_data_by_sel_with_input_param()
{
    std::cout << "\nTesting test_get_data_by_sel_with_input_param() with schema ..."
              << std::endl;

    // It is definite structure, we can know in coding.
    // So we defined in MyDfSchema directly.
    std::vector<MyDataFrameIdxType> idx = {
        123450, 123451, 123452, 123453, 123454, 123455, 123456 };
    std::vector<MyDfSchema::col_1::type> d1 = { 1, 2, 3, 4, 5, 6, 7 };
    std::vector<MyDfSchema::col_2::type> d2 = { 8, 9, 10, 11, 12, 13, 14 };
    std::vector<MyDfSchema::col_3::type> d3 = { 15, 16, 17, 18, 19, 20, 21 };

    // Adding dynamically. Suppose the column name is not known
    // until the process runs. Maybe we only know the column names
    // based on user settings.
    std::vector<double> d4 = { 22, 23, 24, 25 };
    std::vector<std::string> s1 = {"11", "22", "33", "ee", "ff", "gg", "ll" };

    MyDataFrame df;
    df.load_data(std::move(idx),
                 std::make_pair(MyDfSchema::col_1::name, d1),
                 std::make_pair(MyDfSchema::col_2::name, d2),
                 std::make_pair(MyDfSchema::col_3::name, d3));

    // The column names, col4 and col_str, are based on user settings.
    df.load_column("col4",
                   std::move(d4),
                   nan_policy::dont_pad_with_nans);
    df.load_column("col_str",
                   std::move(s1),
                   nan_policy::dont_pad_with_nans);

    auto functor = [](const MyDataFrameIdxType &,
                      const MyDfSchema::col_1::type &val)-> bool {
        return (val >= 5);
    };

    // Just as alias
    CommonColumn<double> col4_alias("col4");
    CommonColumn<std::string> col_str_alias("col_str");
    auto result = df.get_data_by_sel<
        projection_type_list<MyDfSchema::col_1::type,
                             MyDfSchema::col_2::type,
                             MyDfSchema::col_3::type,
                             decltype(col4_alias)::type,
                             decltype(col_str_alias)::type>,
        decltype(functor),
        MyDfSchema::col_1>(functor);

    assert(result.get_index().size() == 3);
    assert(result.get_column<MyDfSchema::col_1>().size() == 3);
    assert(result.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name()).size() == 3);
    assert(result.get_column<decltype(col4_alias)::type>(
               col4_alias.col_name()).size() == 0);
    assert(result.get_index()[0] == 123454);
    assert(result.get_index()[2] == 123456);
    assert(result.get_column<MyDfSchema::col_2>()[1] == 13);
    assert(result.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name())[1] == "gg");
    assert(result.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name())[2] == "ll");
    assert(result.get_column<MyDfSchema::col_1>()[1] == 6);
    assert(result.get_column<MyDfSchema::col_1>()[2] == 7);

    auto functor2 = [](const MyDataFrameIdxType&,
                       const MyDfSchema::col_1::type& val1,
                       const MyDfSchema::col_3::type& val2,
                       const decltype(col_str_alias)::type& val3)-> bool {
        return (val1 >= 5 || val2 == 15 || val3 == "33");
    };
    auto result2 = df.get_data_by_sel<
        projection_type_list<MyDfSchema::col_1::type,
                             MyDfSchema::col_3::type,
                             decltype(col_str_alias)::type>,
        decltype(functor2)>(functor2,
                            MyDfSchema::col_1(),
                            MyDfSchema::col_3(),
                            col_str_alias);

    assert(result2.get_index().size() == 5);
    assert(result2.get_column<MyDfSchema::col_1>().size() == 5);
    assert(result2.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name()).size() == 5);
    assert(result2.get_column<decltype(col4_alias)::type>(
               col4_alias.col_name()).size() == 2);
    assert(result2.get_index()[0] == 123450);
    assert(result2.get_index()[2] == 123454);
    assert(result2.get_index()[4] == 123456);
    assert(result2.get_column<MyDfSchema::col_2>()[0] == 8);
    assert(result2.get_column<MyDfSchema::col_2>()[1] == 10);
    assert(result2.get_column<MyDfSchema::col_2>()[3] == 13);
    assert(result2.get_column<decltype(col4_alias)::type>(
               col4_alias.col_name())[0] == 22);
    assert(result2.get_column<decltype(col4_alias)::type>(
               col4_alias.col_name())[1] == 24);
    assert(result2.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name())[0] == "11");
    assert(result2.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name())[1] == "33");
    assert(result2.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name())[2] == "ff");
    assert(result2.get_column<decltype(col_str_alias)::type>(
               col_str_alias.col_name())[4] == "ll");
    assert(result2.get_column<MyDfSchema::col_1>()[0] == 1);
    assert(result2.get_column<MyDfSchema::col_1>()[1] == 3);
    assert(result2.get_column<MyDfSchema::col_1>()[2] == 5);
}

static void test_get_data_by_sel_without_input_param()  {

    std::cout << "\nTesting test_get_data_by_sel_without_input_param "
                 "with schema ..."
              << std::endl;

    std::vector<MyDataFrameIdxType> idx = {
        123450, 123451, 123452, 123453, 123454, 123455, 123456 };
    std::vector<MyDfSchema::col_1::type> d1 = { 1, 2, 3, 4, 5, 6, 7 };
    std::vector<MyDfSchema::col_2::type> d2 = { 8, 9, 10, 11, 12, 13, 14 };
    std::vector<MyDfSchema::col_3::type> d3 = { 15, 16, 17, 18, 19, 20, 21 };
    std::vector<MyDfSchema::col_4::type> d4 = { 22, 23, 24, 25 };
    std::vector<MyDfSchema::col_str::type> s1 = {
        "11", "22", "33", "ee", "ff", "gg", "ll" };

    MyDataFrame df;
    df.load_data(std::move(idx),
                 std::make_pair(MyDfSchema::col_1::name, d1),
                 std::make_pair(MyDfSchema::col_2::name, d2),
                 std::make_pair(MyDfSchema::col_3::name, d3),
                 std::make_pair(MyDfSchema::col_str::name, s1));
    df.load_column(MyDfSchema::col_4::name,
                   std::move(d4),
                   nan_policy::dont_pad_with_nans);

    auto functor = [](const MyDataFrameIdxType &,
                      const MyDfSchema::col_1::type &val)-> bool {
        return (val >= 5);
    };
    auto result = df.get_data_by_sel<
        projection_list<MyDfSchema::col_1,
                        MyDfSchema::col_2,
                        MyDfSchema::col_3,
                        MyDfSchema::col_4,
                        MyDfSchema::col_str>,
        decltype(functor),
        MyDfSchema::col_1>(functor);

    assert(result.get_index().size() == 3);
    assert(result.get_column<MyDfSchema::col_1>().size() == 3);
    assert(result.get_column<MyDfSchema::col_str>().size() == 3);
    assert(result.get_column<MyDfSchema::col_4>().size() == 0);
    assert(result.get_index()[0] == 123454);
    assert(result.get_index()[2] == 123456);
    assert(result.get_column<MyDfSchema::col_2>()[1] == 13);
    assert(result.get_column<MyDfSchema::col_str>()[1] == "gg");
    assert(result.get_column<MyDfSchema::col_str>()[2] == "ll");
    assert(result.get_column<MyDfSchema::col_1>()[1] == 6);
    assert(result.get_column<MyDfSchema::col_1>()[2] == 7);

    auto functor2 = [](const MyDataFrameIdxType&,
                       const MyDfSchema::col_1::type& val1,
                       const MyDfSchema::col_3::type& val2,
                       const MyDfSchema::col_str::type& val3)-> bool {
        return (val1 >= 5 || val2 == 15 || val3 == "33");
    };
    auto result2 = df.get_data_by_sel<
        projection_list<MyDfSchema::col_1,
                        MyDfSchema::col_3,
                        MyDfSchema::col_str>,
        decltype(functor2),
        MyDfSchema::col_1,
        MyDfSchema::col_3,
        MyDfSchema::col_str>(functor2);

    assert(result2.get_index().size() == 5);
    assert(result2.get_column<MyDfSchema::col_1>().size() == 5);
    assert(result2.get_column<MyDfSchema::col_str>().size() == 5);
    assert(result2.get_column<MyDfSchema::col_4>().size() == 2);
    assert(result2.get_index()[0] == 123450);
    assert(result2.get_index()[2] == 123454);
    assert(result2.get_index()[4] == 123456);
    assert(result2.get_column<MyDfSchema::col_2>()[0] == 8);
    assert(result2.get_column<MyDfSchema::col_2>()[1] == 10);
    assert(result2.get_column<MyDfSchema::col_2>()[3] == 13);
    assert(result2.get_column<MyDfSchema::col_4>()[0] == 22);
    assert(result2.get_column<MyDfSchema::col_4>()[1] == 24);
    assert(result2.get_column<MyDfSchema::col_str>()[0] == "11");
    assert(result2.get_column<MyDfSchema::col_str>()[1] == "33");
    assert(result2.get_column<MyDfSchema::col_str>()[2] == "ff");
    assert(result2.get_column<MyDfSchema::col_str>()[4] == "ll");
    assert(result2.get_column<MyDfSchema::col_1>()[0] == 1);
    assert(result2.get_column<MyDfSchema::col_1>()[1] == 3);
    assert(result2.get_column<MyDfSchema::col_1>()[2] == 5);
}

int main (int, char *[])  {

    test_get_data_by_sel_with_input_param();
    test_get_data_by_sel_without_input_param();

    return (0);
}
