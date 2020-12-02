#include <DataFrame/DataFrame.h>

#include <cassert>
#include <iostream>
#include <string>

using namespace hmdf;

typedef StdDataFrame<unsigned long> MyDataFrame;

static void test_thread_safety()  {

    constexpr size_t    thread_count = 4;

    std::cout << "Testing Thread safety ..." << std::endl;

    SpinLock    lock;

    MyDataFrame::set_lock(&lock);

    std::vector<std::thread>    thr_vec;

    std::cout << "* starting threads ..." << std::endl;
    for (size_t i = 0; i < thread_count; ++i)
        thr_vec.push_back(std::thread([]() {
            constexpr size_t        jobs_per_thread = 2500;
            constexpr unsigned long vec_len = 100;

            for (size_t j = 0; j < jobs_per_thread; ++j) {
                MyDataFrame         df;
                std::vector<double> vec(vec_len);

                std::iota(vec.begin(), vec.end(), 0);
                df.load_index(MyDataFrame::gen_sequence_index(0, vec_len, 1));
                df.load_column("abc", std::move(vec));
            }
        }));

    std::cout << "* waiting for threads to collect results ..." << std::endl;
    for (size_t i = 0; i < thread_count; ++i)
        thr_vec[i].join();

    MyDataFrame::remove_lock();
}

// ---- MAIN ------------------------------------------------------------------

int main(int argc, char *argv[]) {

    test_thread_safety();
    return (0);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
