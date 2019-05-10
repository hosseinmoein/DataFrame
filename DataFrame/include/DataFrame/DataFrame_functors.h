// Hossein Moein
// October 31, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifndef HMDF_DATAFRAME_FUNCTORS_HPP
#define HMDF_DATAFRAME_FUNCTORS_HPP

#include "dllexports/DataFrame_lib_exports.h"

// ----------------------------------------------------------------------------

// This file was factored out so DataFrame.h doesn't become a huge file.
// This was meant to be included inside the private section of DataFrame class.
// This file, by itself, is not useable/compile-able.

// ----------------------------------------------------------------------------

template<typename ... types>
struct consistent_functor_ : DataVec::template visitor_base<types ...>  {

    inline consistent_functor_ (size_type s) : size(s)  {   }

    const DataFrame::size_type  size;
    template<typename T>
    void operator() (T &vec) const;
};

// ----------------------------------------------------------------------------

template<typename T, typename ... types>
struct sort_functor_ : DataVec::template visitor_base<types ...>  {

    inline sort_functor_ (const std::vector<T> &iv) : idx_vec(iv)  {   }

    const std::vector<T> &idx_vec;

    template<typename T2>
    void operator() (T2 &vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct load_functor_ : DataVec::template visitor_base<types ...>  {

    inline load_functor_ (const char *n,
                          std::size_t b,
                          std::size_t e,
                          DataFrame &d)
        : name (n), begin (b), end (e), df(d)  {   }

    const char          *name;
    const std::size_t   begin;
    const std::size_t   end;
    DataFrame           &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct remove_functor_ : DataVec::template visitor_base<types ...>  {

    inline remove_functor_ (std::size_t b, std::size_t e)
        : begin (b), end (e)  {   }

    const std::size_t   begin;
    const std::size_t   end;

    template<typename T>
    void operator() (T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct view_setup_functor_ : DataVec::template visitor_base<types ...>  {

    inline view_setup_functor_ (const char *n,
                                std::size_t b,
                                std::size_t e,
                                DataFrameView<TS> &d)
        : name (n), begin (b), end (e), dfv(d)  {   }

    const char          *name;
    const std::size_t   begin;
    const std::size_t   end;
    DataFrameView<TS>   &dfv;

    template<typename T>
    void operator() (T &vec);
};

template<typename ... types>
friend struct view_setup_functor_;

// ----------------------------------------------------------------------------

template<typename ... types>
struct add_col_functor_ : DataVec::template visitor_base<types ...>  {

    inline add_col_functor_ (const char *n, DataFrame &d)
        : name (n), df(d)  {   }

    const char  *name;
    DataFrame   &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename F, typename ... types>
struct groupby_functor_ : DataVec::template visitor_base<types ...>  {

    inline groupby_functor_ (const char *n,
                             std::size_t b,
                             std::size_t e,
                             const TS &ts,
                             F &f,
                             DataFrame &d)
        : name(n), begin(b), end(e), indices(ts), functor(f), df(d) {  }

    const char          *name;
    const std::size_t   begin;
    const std::size_t   end;
    const TS            &indices;
    F                   &functor;
    DataFrame           &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename F, typename ... types>
struct bucket_functor_ : DataVec::template visitor_base<types ...>  {

    inline bucket_functor_ (const char *n,
                            const TSVec &ts,
                            const TimeStamp &i,
                            F &f,
                            DataFrame &d)
        : name(n), indices(ts), interval(i), functor(f), df(d) {  }

    const char      *name;
    const TSVec     &indices;
    const TimeStamp &interval;
    F               &functor;
    DataFrame       &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct print_functor_ : DataVec::template visitor_base<types ...>  {

    inline print_functor_ (const char *n, bool vo, std::ostream &o)
        : name(n), values_only(vo), os(o)  {   }

    const char      *name;
    const bool      values_only;
    std::ostream    &os;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct equal_functor_ : DataVec::template visitor_base<types ...>  {

    inline equal_functor_ (const char *n, const DataFrame &d)
        : name(n), df(d)  {  }

    const char      *name;
    const DataFrame &df;
    bool            result { true };

    template<typename T>
    void operator() (const std::vector<T> &lhs_vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct mod_by_idx_functor_ : DataVec::template visitor_base<types ...>  {

    inline mod_by_idx_functor_ (const char *n,
                                const DataFrame &d,
                                size_type li,
                                size_type ri)
        : name(n), rhs_df(d), lhs_idx(li), rhs_idx(ri)  {  }

    const char      *name;
    const DataFrame &rhs_df;
    const size_type lhs_idx;
    const size_type rhs_idx;

    template<typename T>
    void operator() (std::vector<T> &lhs_vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct index_join_functor_common_ : DataVec::template visitor_base<types ...>  {

    inline index_join_functor_common_ (
        const char *n,
        const DataFrame &r,
        const IndexIdxVector &mii,
        DataFrame &res)
        : name(n), rhs(r), joined_index_idx(mii), result(res)  {  }

    const char              *name;
    const DataFrame         &rhs;
    const IndexIdxVector    &joined_index_idx;
    DataFrame               &result;

    template<typename T>
    void operator() (const std::vector<T> &lhs_vec);
};

// ----------------------------------------------------------------------------

// SIDE:  0 = Left, 1 = Right
template<int SIDE, typename ... types>
struct index_join_functor_oneside_
    : DataVec::template visitor_base<types ...>  {

    inline index_join_functor_oneside_ (
        const char *n,
        const IndexIdxVector &mii,
        DataFrame &res)
        : name(n), joined_index_idx(mii), result(res)  {  }

    const char              *name;
    const IndexIdxVector    &joined_index_idx;
    DataFrame               &result;

    template<typename T>
    void operator() (const std::vector<T> &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct vertical_shift_functor_ : DataVec::template visitor_base<types ...>  {

    inline vertical_shift_functor_ (size_type periods, shift_policy sh_po)
        : n(periods), sp(sh_po)  {   }

    const DataFrame::size_type  n;
    const shift_policy          sp;

    template<typename T>
    void operator() (T &vec) const;
};

template<typename ... types>
friend struct vertical_shift_functor_;

// ----------------------------------------------------------------------------

template<typename ... types>
struct rotate_functor_ : DataVec::template visitor_base<types ...>  {

    inline rotate_functor_ (size_type periods, shift_policy sh_po)
        : n(periods), sp(sh_po)  {   }

    const DataFrame::size_type  n;
    const shift_policy          sp;

    template<typename T>
    void operator() (T &vec) const;
};

template<typename ... types>
friend struct rotate_functor_;

// ----------------------------------------------------------------------------

template<typename TST, template<typename> class OPT, typename ... types>
struct operator_functor_ : DataVec::template visitor_base<types ...>  {

    inline operator_functor_ (const std::vector<TST> &lhsidx,
                              const std::vector<TST> &rhsidx,
                              const std::vector<TST> &newidx,
                              const StdDataFrame<TST> &rhsdf,
                              const char *colname,
                              StdDataFrame<TST> &resultdf)
         : lhs_idx(lhsidx),
           rhs_idx(rhsidx),
           new_idx(newidx),
           rhs_df(rhsdf),
           col_name(colname),
           result_df(resultdf)  {  }

    const std::vector<TST>  &lhs_idx;
    const std::vector<TST>  &rhs_idx;
    const std::vector<TST>  &new_idx;
    const StdDataFrame<TST> &rhs_df;
    const char              *col_name;
    StdDataFrame<TST>       &result_df;

    template<typename T>
    void operator() (const std::vector<T> &lhs_vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct map_missing_rows_functor_ :
    DataVec::template visitor_base<types ...>  {

    inline map_missing_rows_functor_ (size_type ir, DropRowMap &mrm)
        : index_rows(ir), missing_row_map(mrm)  {   }

    const size_type index_rows;
    DropRowMap      &missing_row_map;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct drop_missing_rows_functor_ :
    DataVec::template visitor_base<types ...>  {

    inline drop_missing_rows_functor_ (const DropRowMap &mrm,
                                       drop_policy p,
                                       size_type th,
                                       size_type cn)
        : missing_row_map(mrm),
          policy(p),
          threshold(th),
          col_num(cn)  {   }

    const DropRowMap    &missing_row_map;
    const drop_policy   policy;
    const size_type     threshold;
    const size_type     col_num;

    template<typename T>
    void operator() (T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... types>
struct get_row_functor_ : DataVec::template visitor_base<types ...>  {

    inline get_row_functor_ (HeteroVector &r, size_type rn)
        : result(r), row_num(rn)  {   }

    HeteroVector    &result;
    const size_type row_num;

    template<typename T>
    void operator() (T &vec);
};

#endif //HMDF_DATAFRAME_FUNCTORS_HPP

// ----------------------------------------------------------------------------

// Local Variables:
// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
