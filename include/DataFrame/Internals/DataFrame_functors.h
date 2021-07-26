// Hossein Moein
// October 31, 2018
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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

template<typename ... Ts>
struct sort_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline sort_functor_ (const std::vector<size_t> &si, size_t is)
        : sorted_idxs(si), idx_s(is)  {   }

    const std::vector<size_t>   &sorted_idxs;
    std::vector<size_t>         sorted_idxs_copy;
    const size_t                idx_s;

    template<typename T2>
    void operator() (T2 &vec);
};

// ----------------------------------------------------------------------------

template<typename LHS, typename ... Ts>
struct load_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline load_functor_ (const char *n,
                          std::size_t b,
                          std::size_t e,
                          LHS &d,
                          nan_policy np = nan_policy::pad_with_nans)
    : name (n), begin (b), end (e), df(d), nan_p(np)  {   }

    const char          *name;
    const std::size_t   begin;
    const std::size_t   end;
    LHS                 &df;
    const nan_policy    nan_p;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct load_all_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline load_all_functor_ (const char *n, DataFrame &d) : name(n), df(d) { }

    const char  *name;
    DataFrame   &df;

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

template<typename LHS, typename ... Ts>
struct view_setup_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline view_setup_functor_ (const char *n,
                                std::size_t b,
                                std::size_t e,
                                LHS &d)
        : name (n), begin (b), end (e), dfv(d)  {   }

    const char          *name;
    const std::size_t   begin;
    const std::size_t   end;
    LHS                 &dfv;

    template<typename T>
    void operator() (T &vec);
};

template<typename LHS, typename ... Ts>
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

template<typename ... Ts>
struct print_csv_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline print_csv_functor_ (const char *n, std::ostream &o)
        : name(n), os(o)  {   }

    const char      *name;
    std::ostream    &os;

    template<typename T>
    void operator() (const T &vec);
};


// ----------------------------------------------------------------------------

template<typename ... Ts>
struct print_json_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline print_json_functor_ (const char *n, bool npc, std::ostream &o)
        : name(n), need_pre_comma(npc), os(o)  {   }

    const char      *name;
    const bool      need_pre_comma;
    std::ostream    &os;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename S, typename ... Ts>
struct print_csv2_header_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline print_csv2_header_functor_ (const char *n, S &o)
        : name(n), os(o)  {   }

    const char  *name;
    S           &os;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename S, typename ... Ts>
struct print_csv2_data_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline print_csv2_data_functor_ (std::size_t i, S &o) : index(i), os(o) {  }

    const std::size_t   index;
    S                   &os;

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
    void operator() (const T &lhs_vec);
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
    void operator() (T &lhs_vec) const;
};

// ----------------------------------------------------------------------------

template<typename RES_T, typename ... Ts>
struct  index_join_functor_common_ : DataVec::template visitor_base<Ts ...>  {

    inline index_join_functor_common_ (
        const char *n,
        const DataFrame &r,
        const IndexIdxVector &mii,
        RES_T &res)
        : name(n), rhs(r), joined_index_idx(mii), result(res)  {  }

    const char              *name;
    const DataFrame         &rhs;
    const IndexIdxVector    &joined_index_idx;
    RES_T                   &result;

    template<typename T>
    void operator() (const T &lhs_vec);
};

// ----------------------------------------------------------------------------

// SIDE:  0 = Left, 1 = Right
template<int SIDE, typename RES_T, typename ... Ts>
struct  index_join_functor_oneside_
    : DataVec::template visitor_base<Ts ...>  {

    inline index_join_functor_oneside_ (
        const char *n,
        const IndexIdxVector &mii,
        RES_T &res)
        : name(n), joined_index_idx(mii), result(res)  {  }

    const char              *name;
    const IndexIdxVector    &joined_index_idx;
    RES_T                   &result;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename RES_T, typename ... Ts>
struct  concat_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline concat_functor_ (const char *n, RES_T &res, bool ic, size_type ois)
        : name(n), result(res), insert_col(ic), original_index_s(ois)  {  }

    const char      *name;
    RES_T           &result;
    const bool      insert_col;
    const size_type original_index_s;

    template<typename T>
    void operator() (const T &vec);
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
    void operator() (const T &lhs_vec);
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
    void operator() (const T &vec);
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
    void operator() (T &vec);
};

// ----------------------------------------------------------------------------

template<typename LHS, typename ... Ts>
struct concat_load_view_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline concat_load_view_functor_ (const char *n, LHS &r)
        : name (n), result(r)  {   }

    const char  *name;
    LHS         &result;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct sel_remove_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline sel_remove_functor_ (const std::vector<size_type> &si)
        : sel_indices (si)  {   }

    const std::vector<size_type>    &sel_indices;

    template<typename T>
    void operator() (T &vec) const;
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct shuffle_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline shuffle_functor_ ()  {  }

    template<typename T>
    void operator() (T &vec) const;
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

template<typename ... Ts>
struct columns_info_functor_ : DataVec::template visitor_base<Ts ...>  {

    using result_t =
        std::vector<std::tuple<ColNameType, size_type, std::type_index>>;

    inline columns_info_functor_ (result_t &r, const char *n)
        : result(r), name(n)  {   }

    result_t    &result;
    const char  *name;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename ... Ts>
struct copy_remove_functor_ : DataVec::template visitor_base<Ts ...>  {

    inline copy_remove_functor_ (const char *n,
                                 const std::vector<std::size_t>  &td,
                                 DataFrame &d)
        : name(n), to_delete (td), df(d)  {   }

    const char                      *name;
    const std::vector<std::size_t>  &to_delete;
    DataFrame                       &df;

    template<typename T>
    void operator() (const T &vec);
};

// ----------------------------------------------------------------------------

template<typename DF, typename ... Ts>
struct fill_missing_functor_ :
    DataVec::template visitor_base<Ts ...>  {

    inline fill_missing_functor_ (const IndexVecType &sidx,
                                  const IndexVecType &ridx,
                                  const DF &r,
                                  const char *cname)
        : self_idx(sidx), rhs_idx(ridx), rhs(r), col_name(cname)  {   }

    const IndexVecType  &self_idx;;
    const IndexVecType  &rhs_idx;;
    const DF            &rhs;
    const char          *col_name;

    template<typename T>
    void operator() (T &vec);
};

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
