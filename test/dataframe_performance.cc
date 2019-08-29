#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameVisitors.h>
#include <DataFrame/RandGen.h>

#include <iostream>

using namespace hmdf;

typedef StdDataFrame<time_t> MyDataFrame;

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    MyDataFrame             df;
    const size_t            index_sz =
        df.load_index(
            MyDataFrame::gen_datetime_index("01/01/1970",
                                            "08/15/2019",
                                            time_frequency::secondly,
                                            1));
    RandGenParams<double>   p;

    p.mean = 1.0;  // Default
    p.std = 0.005;

    df.load_column("normal", gen_normal_dist<double>(index_sz, p));
    df.load_column("log_normal", gen_lognormal_dist<double>(index_sz));
    p.lambda = 1.5;
    df.load_column("exponential", gen_exponential_dist<double>(index_sz, p));

    std::cout << "All memory allocations are done. Calculating means ..."
              << std::endl;

    MeanVisitor<double, time_t> n_mv;
    MeanVisitor<double, time_t> ln_mv;
    MeanVisitor<double, time_t> e_mv;

    df.visit<double>("normal", n_mv);
    df.visit<double>("log_normal", ln_mv);
    df.visit<double>("exponential", e_mv);

    // std::cout << "Normal mean " << n_mv.get_result() << std::endl;
    // std::cout << "Log Normal mean " << ln_mv.get_result() << std::endl;
    // std::cout << "Exponential mean " << e_mv.get_result() << std::endl;
    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
