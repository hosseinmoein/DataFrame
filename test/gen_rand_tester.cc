#include <iostream>
#include <cassert>

#include <DataFrame/RandGen.h>

using namespace hmdf;

// -----------------------------------------------------------------------------

int main(int argc, char *argv[]) {

    {
        RandGenParams<long> p;

        p.min_value = 0;
        p.max_value = 1000000;

        auto    r = gen_uniform_int_distribution<long>(1024, p);
    }

    {
        RandGenParams<double> p;

        p.min_value = 0;
        p.max_value = 2.0;
        p.seed = 23;

        auto    r = gen_uniform_real_distribution<double>(1024, p);
    }

    {
        auto    r = gen_bernoulli_distribution(1024);
    }

    {
        RandGenParams<int>  p;

        p.t_dist = 1000;

        auto    r = gen_binomial_distribution<int>(1024, p);

        for (auto citer : r)
            std::cout << citer << ", ";
        std::cout << std::endl;
    }

    return (0);
}

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
