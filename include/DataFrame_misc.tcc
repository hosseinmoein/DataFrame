// Hossein Moein
// September 12, 2017

#include <DataFrame.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::consistent_functor_<Ts ...>::operator() (T &vec) const  {

    using ValueType =
        typename std::remove_reference<decltype(vec)>::type::value_type;

    vec.resize(size, ValueType());
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename T, typename ... Ts>
template<typename T2>
void
DataFrame<TS, DS>::sort_functor_<T, Ts ...>::operator() (T2 &vec) const  {

    using VecType = typename std::remove_reference<decltype(vec)>::type;
    using DataValueType = typename VecType::value_type;

    std::multimap<T, DataValueType> tmp_map;
    const DataFrame::size_type      idx_s = idx_vec.size();

    for (size_t i = 0; i < idx_s; ++i)
        tmp_map.emplace(std::move(idx_vec[i]), std::move(vec[i]));
    vec.clear();
    for (auto &iter : tmp_map)
        vec.emplace_back(std::move(iter.second));

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::load_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.load_column<ValueType>(name,
                              vec.begin() + begin,
                              vec.begin() + end,
                              true);
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::add_col_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    df.create_column<ValueType>(name);
    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename F, typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::groupby_functor_<F, Ts ...>::operator() (const T &vec)  {

    for (std::size_t i = begin; i < end && i < vec.size(); ++i)
        functor (timestamp, name, vec[i]);

    if (! ::strcmp(name, "INDEX"))  {
        TimeStamp   v;

        functor.get_value(v);
        df.append_index(v);
    }
    else  {
        using VecType = typename std::remove_reference<T>::type;
        using ValueType = typename VecType::value_type;

        ValueType   v;

        functor.get_value(v);
        df.append_column<ValueType>(name, v, false);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename F, typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::bucket_functor_<F, Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    const std::size_t   ts_s = timestamps.size();
    std::size_t         marker = 0;

    if (df.timestamps_.empty())
        for (std::size_t i = 0; i < ts_s; ++i)
            if (timestamps[i] - timestamps[marker] >= interval)  {
                df.timestamps_.push_back(timestamps[i - 1]);
                marker = i;
            }

    for (std::size_t i = 0, marker = 0; i < ts_s; ++i)  {
        if (timestamps[i] - timestamps[marker] >= interval)  {
            ValueType   v;

            functor.get_value(v);
            df.append_column<ValueType>(name, v, false);
            functor.reset();
            marker = i;
        }
        functor (timestamps[i], name, vec[i]);
    }

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::print_functor_<Ts ...>::operator() (const T &vec)  {

    using VecType = typename std::remove_reference<T>::type;
    using ValueType = typename VecType::value_type;

    if (! values_only)  {
        os << name << ':';
        if (typeid(ValueType) == typeid(double))
            os << "<double>:";
        else if (typeid(ValueType) == typeid(int))
            os << "<int>:";
        else if (typeid(ValueType) == typeid(unsigned int))
            os << "<uint>:";
        else if (typeid(ValueType) == typeid(long))
            os << "<long>:";
        else if (typeid(ValueType) == typeid(unsigned long))
            os << "<ulong>:";
        else if (typeid(ValueType) == typeid(std::string))
            os << "<string>:";
        else if (typeid(ValueType) == typeid(bool))
            os << "<bool>:";
        else
            os << "<N/A>:";
    }
    for (std::size_t i = 0; i < vec.size(); ++i)
        os << vec[i] << ',';
    os << '\n';

    return;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::
equal_functor_<Ts ...>::operator() (const DS<T> &lhs_vec)  {

    const auto  &iter = df.data_tb_.find(name);

    if (iter == df.data_tb_.end())  {
        result = false;
        return;
    }
	
    const DataVec   &hv = df.data_[iter->second];
    const DS<T>     &rhs_vec = hv.get_vec<T, DS<T>>();

    if (lhs_vec != rhs_vec)
        result = false;
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts>
template<typename T>
void
DataFrame<TS, DS>::
mod_by_idx_functor_<Ts ...>::operator() (DS<T> &lhs_vec) const  {

    const auto  &iter = rhs_df.data_tb_.find(name);

	if (iter != rhs_df.data_tb_.end())  {
        const DS<T> &rhs_vec = rhs_df.get_column<T>(name);

		lhs_vec[lhs_idx] = rhs_vec[rhs_idx];
	}
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts, typename F, std::size_t ... Is>
void
DataFrame<TS, DS>::for_each_in_tuple_ (const std::tuple<Ts ...> &tu,
                                       F func,
                                       std::index_sequence<Is ...>) const  {

    using expander = int[];
    (void) expander { 0, (func(std::get<Is>(tu)), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts, typename F, std::size_t ... Is>
void
DataFrame<TS, DS>::for_each_in_tuple_ (std::tuple<Ts ...> &tu,
                                       F func,
                                       std::index_sequence<Is ...>)  {

    using expander = int[];
    (void) expander { 0, (func(std::get<Is>(tu)), 0) ... };
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts, typename F>
void
DataFrame<TS, DS>::
for_each_in_tuple_ (const std::tuple<Ts...> &tu, F func) const  {

    for_each_in_tuple_(tu, func, std::make_index_sequence<sizeof...(Ts)>());
}

// ----------------------------------------------------------------------------

template<typename TS, template<typename DT, class... types> class DS>
template<typename ... Ts, typename F>
void
DataFrame<TS, DS>::for_each_in_tuple_ (std::tuple<Ts...> &tu, F func)  {

    for_each_in_tuple_(tu, func, std::make_index_sequence<sizeof...(Ts)>());
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
