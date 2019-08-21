// Hossein Moein
// October 31, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

// ----------------------------------------------------------------------------

// This file was factored out so DataFrame.h doesn't become a huge file.
// This was meant to be included inside the private section of DataFrame class.
// This file, by itself, is not useable/compile-able.

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct consistent_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline consistent_functor_ (size_type s) : size(s)  {   }

    const DataFrame::size_type  size;
    template<typename T>
    void operator() (T &vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct shrink_to_fit_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline shrink_to_fit_functor_ ()  {   }

    template<typename T>
    void operator() (T &vec) const;
};

// ----------------------------------------------------------------------------

template<typename T, typename ... Ts>
struct sort_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline sort_functor_ (const std::vector<T> &iv) : idx_vec(iv)  {   }

    const std::vector<T> &idx_vec;

    template<typename T2>
    void operator() (T2 &vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct load_functor_ : DataVec::template visitor_base<Ts ...>  {

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

template<typename ... Ts>
struct remove_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline remove_functor_ (std::size_t b, std::size_t e)
        : begin (b), end (e)  {   }

    const std::size_t   begin;
    const std::size_t   end;

    template<typename T>
    void operator() (T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct view_setup_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline view_setup_functor_ (const char *n,
                                std::size_t b,
                                std::size_t e,
                                DataFrameView<IndexType> &d)
        : name (n), begin (b), end (e), dfv(d)  {   }

    const char                  *name;
    const std::size_t           begin;
    const std::size_t           end;
    DataFrameView<IndexType>    &dfv;

    template<typename T>
    void operator() (T &vec);
};

template<typename ... Ts>
friend struct view_setup_functor_;

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct add_col_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline add_col_functor_ (const char *n, DataFrame &d)
        : name (n), df(d)  {   }

    const char  *name;
    DataFrame   &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename F, typename ... Ts>
struct groupby_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline groupby_functor_ (const char *n,
                             std::size_t b,
                             std::size_t e,
                             const IndexType &ts,
                             F &f,
                             DataFrame &d)
        : name(n), begin(b), end(e), indices(ts), functor(f), df(d) {  }

    const char          *name;
    const std::size_t   begin;
    const std::size_t   end;
    const IndexType     &indices;
    F                   &functor;
    DataFrame           &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename F, typename ... Ts>
struct bucket_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline bucket_functor_ (const char *n,
                            const IndexVecType &ts,
                            const IndexType &i,
                            F &f,
                            DataFrame &d)
        : name(n), indices(ts), interval(i), functor(f), df(d) {  }

    const char          *name;
    const IndexVecType  &indices;
    const IndexType     &interval;
    F                   &functor;
    DataFrame           &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct print_csv_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline print_csv_functor_ (const char *n, bool vo, std::ostream &o)
        : name(n), values_only(vo), os(o)  {   }

    const char      *name;
    const bool      values_only;
    std::ostream    &os;

    template<typename T>
    void operator() (const T &vec);
};


// ----------------------------------------------------------------------------

template<typename ... Ts>
struct print_json_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline print_json_functor_ (const char *n,
                                bool vo,
                                bool npc,
                                std::ostream &o)
        : name(n), values_only(vo), need_pre_comma(npc), os(o)  {   }

    const char      *name;
    const bool      values_only;
    const bool      need_pre_comma;
    std::ostream    &os;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct equal_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline equal_functor_ (const char *n, const DataFrame &d)
        : name(n), df(d)  {  }

    const char      *name;
    const DataFrame &df;
    bool            result { true };

    template<typename T>
    void operator() (const std::vector<T> &lhs_vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct mod_by_idx_functor_ : DataVec::template visitor_base<Ts ...>  {

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

template<typename ... Ts>
struct index_join_functor_common_ : DataVec::template visitor_base<Ts ...>  {

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
template<int SIDE, typename ... Ts>
struct index_join_functor_oneside_
    : DataVec::template visitor_base<Ts ...>  {

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

template<typename ... Ts>
struct vertical_shift_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline vertical_shift_functor_ (size_type periods, shift_policy sh_po)
        : n(periods), sp(sh_po)  {   }

    const DataFrame::size_type  n;
    const shift_policy          sp;

    template<typename T>
    void operator() (T &vec) const;
};

template<typename ... Ts>
friend struct vertical_shift_functor_;

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct rotate_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline rotate_functor_ (size_type periods, shift_policy sh_po)
        : n(periods), sp(sh_po)  {   }

    const DataFrame::size_type  n;
    const shift_policy          sp;

    template<typename T>
    void operator() (T &vec) const;
};

template<typename ... Ts>
friend struct rotate_functor_;

// ----------------------------------------------------------------------------

template<typename IDX, template<typename> class OPT, typename ... Ts>
struct operator_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline operator_functor_ (const std::vector<IDX> &lhsidx,
                              const std::vector<IDX> &rhsidx,
                              const std::vector<IDX> &newidx,
                              const StdDataFrame<IDX> &rhsdf,
                              const char *colname,
                              StdDataFrame<IDX> &resultdf)
         : lhs_idx(lhsidx),
           rhs_idx(rhsidx),
           new_idx(newidx),
           rhs_df(rhsdf),
           col_name(colname),
           result_df(resultdf)  {  }

    const std::vector<IDX>  &lhs_idx;
    const std::vector<IDX>  &rhs_idx;
    const std::vector<IDX>  &new_idx;
    const StdDataFrame<IDX> &rhs_df;
    const char              *col_name;
    StdDataFrame<IDX>       &result_df;

    template<typename T>
    void operator() (const std::vector<T> &lhs_vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct map_missing_rows_functor_ :
    DataVec::template visitor_base<Ts ...>  {

    inline map_missing_rows_functor_ (size_type ir, DropRowMap &mrm)
        : index_rows(ir), missing_row_map(mrm)  {   }

    const size_type index_rows;
    DropRowMap      &missing_row_map;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct drop_missing_rows_functor_ :
    DataVec::template visitor_base<Ts ...>  {

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

template<typename ... Ts>
struct get_row_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline get_row_functor_ (HeteroVector &r, size_type rn)
        : result(r), row_num(rn)  {   }

    HeteroVector    &result;
    const size_type row_num;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename IT, typename ... Ts>
struct sel_load_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline sel_load_functor_ (const char *n,
                              const std::vector<IT> &si,
                              size_type is,
                              DataFrame &d)
        : name (n), sel_indices (si), indices_size(is), df(d)  {   }

    const char              *name;
    const std::vector<IT>   &sel_indices;
    const size_type         indices_size;
    DataFrame               &df;

    template<typename T>
    void operator() (const std::vector<T> &vec);
};

// ----------------------------------------------------------------------------

template<typename IT, typename ... Ts>
struct sel_load_view_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline sel_load_view_functor_ (const char *n,
                                   const std::vector<IT> &si,
                                   size_type is,
                                   DataFramePtrView<IndexType> &d)
        : name (n), sel_indices (si), indices_size(is), dfv(d)  {   }

    const char                  *name;
    const std::vector<IT>       &sel_indices;
    const size_type             indices_size;
    DataFramePtrView<IndexType> &dfv;

    template<typename T>
    void operator() (std::vector<T> &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct sel_remove_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline sel_remove_functor_ (const std::vector<size_type> &si)
        : sel_indices (si)  {   }

    const std::vector<size_type>    &sel_indices;

    template<typename T>
    void operator() (std::vector<T> &vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct shuffle_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline shuffle_functor_ ()  {  }

    template<typename T>
    void operator() (std::vector<T> &vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct random_load_data_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline random_load_data_functor_ (
        const char *n,
        const std::vector<std::size_t>  &ri,
        DataFrame &d)
        : name (n), rand_indices (ri), df(d)  {   }

    const char                      *name;
    const std::vector<std::size_t>  &rand_indices;
    DataFrame                       &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct random_load_view_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline random_load_view_functor_ (
        const char *n,
        const std::vector<std::size_t>  &ri,
        DataFramePtrView<IndexType> &d)
        : name (n), rand_indices (ri), dfv(d)  {   }

    const char                      *name;
    const std::vector<std::size_t>  &rand_indices;
    DataFramePtrView<IndexType>     &dfv;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
