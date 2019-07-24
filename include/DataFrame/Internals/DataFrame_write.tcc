// Hossein Moein
// July 24, 2019
// Copyright (C) 2019-2022 Hossein Moein
// Distributed under the BSD Software License (see file License)

#include <DataFrame/DataFrame.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

template<typename S, typename T>
inline static S &_write_df_index_(S &o, const T &value)  {

    return (o << value);
}

template<typename S>
inline static S &_write_df_index_(S &o, const DateTime &value)  {

    return (o << value.time() << '.' << value.nanosec());
}

template<typename I, typename H>
template<typename S, typename ...Ts>
bool DataFrame<I, H>::
write (S &o, bool values_only, io_format iof) const  {

    if (iof != io_format::csv && iof != io_format::json)
        throw NotImplemented("write(): This io_format is not implemented");

    if (iof == io_format::json)
        o << "{\n";

    if (! values_only)  {
        if (iof == io_format::json)
            o << "\"INDEX\":{\"N\"=" << indices_.size() << ',';
        else
            o << "INDEX:" << indices_.size() << ':';

        if (typeid(IndexType) == typeid(float))
            o << (iof == io_format::csv ? "<float>:" : "\"T\"=\"float\",");
        else if (typeid(IndexType) == typeid(double))
            o << (iof == io_format::csv ? "<double>:" : "\"T\"=\"double\",");
        else if (typeid(IndexType) == typeid(long double))
            o << (iof == io_format::csv
                      ? "<longdouble>:"
                      : "\"T\"=\"longdouble\",");
        else if (typeid(IndexType) == typeid(short int))
            o << (iof == io_format::csv ? "<short>:" : "\"T\"=\"short\",");
        else if (typeid(IndexType) == typeid(unsigned short int))
            o << (iof == io_format::csv ? "<ushort>:" : "\"T\"=\"ushort\",");
        else if (typeid(IndexType) == typeid(int))
            o << (iof == io_format::csv ? "<int>:" : "\"T\"=\"int\",");
        else if (typeid(IndexType) == typeid(unsigned int))
            o << (iof == io_format::csv ? "<uint>:" : "\"T\"=\"uint\",");
        else if (typeid(IndexType) == typeid(long int))
            o << (iof == io_format::csv ? "<long>:" : "\"T\"=\"long\",");
        else if (typeid(IndexType) == typeid(long long int))
            o << (iof == io_format::csv
                      ? "<longlong>:"
                      : "\"T\"=\"longlong\",");
        else if (typeid(IndexType) == typeid(unsigned long int))
            o << (iof == io_format::csv ? "<ulong>:" : "\"T\"=\"ulong\",");
        else if (typeid(IndexType) == typeid(unsigned long long int))
            o << (iof == io_format::csv
                      ? "<ulonglong>:"
                      : "\"T\"=\"ulonglong\",");
        else if (typeid(IndexType) == typeid(std::string))
            o << (iof == io_format::csv ? "<string>:" : "\"T\"=\"string\",");
        else if (typeid(IndexType) == typeid(bool))
            o << (iof == io_format::csv ? "<bool>:" : "\"T\"=\"bool\",");
        else if (typeid(IndexType) == typeid(DateTime))
            o << (iof == io_format::csv
                      ? "<DateTime>:"
                      : "\"T\"=\"DateTime\",");
        else
            o << (iof == io_format::csv ? "<N/A>:" : "\"T\"=\"N/A\",");

        if (iof == io_format::json)  {
            o << "}\n";
        }
        else  {
            for (size_type i = 0; i < indices_.size(); ++i)
                _write_df_index_(o, indices_[i]) << ',';
            o << '\n';
        }
    }

    for (const auto &iter : column_tb_)  {
        print_functor_<Ts ...>  functor (iter.first.c_str(), values_only, o);

        data_[iter.second].change(functor);
    }

    if (iof == io_format::json)
        o << "}";
    o << std::endl;
    return (true);
}

// ----------------------------------------------------------------------------

template<typename I, typename H>
template<typename S, typename ...Ts>
std::future<bool> DataFrame<I, H>::
write_async (S &o, bool values_only, io_format iof) const  {

    return (std::async(std::launch::async,
                       &DataFrame::write<S, Ts ...>,
                       this,
                       std::ref(o),
                       values_only,
                       iof));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
