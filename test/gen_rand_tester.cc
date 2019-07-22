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
    }

    {
        RandGenParams<int>  p;

        p.t_dist = 1000;

        auto    r = gen_negative_binomial_distribution<int>(1024, p);
    }

    {
        auto    r = gen_geometric_distribution<int>(1024);
    }

    {
        RandGenParams<int>  p;

        p.mean = 4.0;

        auto    r = gen_poisson_distribution<int>(1024, p);
    }

    {
        RandGenParams<double>   p;

        p.lambda = 1.5;

        auto    r = gen_exponential_distribution<double>(1024, p);
    }

    {
        RandGenParams<double>   p;

        p.alpha = 1.5;
        p.beta = 1.5;

        auto    r = gen_gamma_distribution<double>(1024, p);
    }

    {
        RandGenParams<double>   p;

        p.alpha = 1.5;
        p.beta = 1.5;

        auto    r = gen_weibull_distribution<double>(1024, p);
    }

    {
        RandGenParams<double>   p;

        p.alpha = 1.5;
        p.beta = 1.5;

        auto    r = gen_extreme_value_distribution<double>(1024, p);

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
