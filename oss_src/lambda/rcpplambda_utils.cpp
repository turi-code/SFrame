/*
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "rcpplambda_utils.hpp"
#include <Rcpp.h>

namespace Rcpp {

template <> graphlab::flexible_type as(SEXP value) {
    if (Rf_length(value) > 1) {
        std::vector<graphlab::flexible_type> values(Rf_length(value));
        switch( TYPEOF(value) ) {
        case REALSXP: {
            Rcpp::NumericVector tmp = Rcpp::as<Rcpp::NumericVector>(value);
            for (size_t i = 0; i < tmp.size(); i++) {
                if (!ISNA(REAL(value)[i])) {
                    values[i] = tmp[i];
                } else {
                    values[i] = graphlab::FLEX_UNDEFINED;
                }
            }
            break;
        }
        case INTSXP: {
            if( Rf_isFactor(value) ) {
                Rcpp::stop("incompatible types, factor type is not support.");
            }
            Rcpp::IntegerVector tmp = Rcpp::as<Rcpp::IntegerVector>(value);
            for (size_t i = 0; i < tmp.size(); i++) {
                if (INTEGER(value)[i] != NA_INTEGER) {
                    values[i] = tmp[i];
                } else {
                    values[i] = graphlab::FLEX_UNDEFINED;
                }
            }
            break;
        }
        case LGLSXP: {
            //Rcpp::Rcout << "logical type has been converted into int" << std::endl;
            Rcpp::IntegerVector tmp = Rcpp::as<Rcpp::IntegerVector>(value);
            for (size_t i = 0; i < tmp.size(); i++) {
                if (LOGICAL(value)[i] != NA_LOGICAL) {
                    values[i] = tmp[i];
                } else {
                    values[i] = graphlab::FLEX_UNDEFINED;
                }
            }
            break;
        }
        case STRSXP: {
            Rcpp::CharacterVector tmp = Rcpp::as<Rcpp::CharacterVector>(value);
            for (size_t i = 0; i < tmp.size(); i++) {
                if (STRING_ELT(value, i) != NA_STRING) {
                    values[i] = Rcpp::as<std::string>(tmp[i]);
                } else {
                    values[i] = graphlab::FLEX_UNDEFINED;
                }
            }
            break;
        }
        case CHARSXP: {
            Rcpp::CharacterVector tmp = Rcpp::as<Rcpp::CharacterVector>(value);
            for (size_t i = 0; i < tmp.size(); i++) {
                if (tmp[i] != NA_STRING) {
                    values[i] = Rcpp::as<std::string>(tmp[i]);
                } else {
                    values[i] = graphlab::FLEX_UNDEFINED;
                }
            }
            break;
        }
        case VECSXP: {
            Rcpp::List lst(value);
            for (size_t i = 0; i < values.size(); i++) {
                if (TYPEOF(lst[i]) == VECSXP) {
                    values[i] = Rcpp::as<graphlab::flexible_type>(lst[i]);
                } else {
                    values[i] = Rcpp::as<graphlab::flexible_type>(lst[i]);
                }
            }
            break;
        }
        default: {
            Rcpp::stop("incompatible types encountered");
        }
        }
        return values;
    } else {
        graphlab::flexible_type flex_value;
        switch( TYPEOF(value) ) {
        case REALSXP: {
            if (!ISNA(REAL(value)[0])) {
                flex_value = Rcpp::as<double>(value);
            } else {
                flex_value = graphlab::FLEX_UNDEFINED;
            }
            break;
        }
        case INTSXP: {
            if (INTEGER(value)[0] != NA_INTEGER) {
                flex_value = Rcpp::as<int>(value);
            } else {
                flex_value = graphlab::FLEX_UNDEFINED;
            }
            break;
        }
        case LGLSXP: {
            //Rcpp::Rcout << "logical type has been converted into int" << std::endl;
            if (LOGICAL(value)[0] != NA_LOGICAL) {
                flex_value = Rcpp::as<int>(value);
            } else {
                flex_value = graphlab::FLEX_UNDEFINED;
            }
            break;
        }
        case STRSXP: {
            if (STRING_ELT(value, 0) != NA_STRING) {
                flex_value = Rcpp::as<std::string>(value);
            } else {
                flex_value = graphlab::FLEX_UNDEFINED;
            }
            break;
        }
        case CHARSXP: {
            if (value != NA_STRING) {
                flex_value = Rcpp::as<std::string>(value);
            } else {
                flex_value = graphlab::FLEX_UNDEFINED;
            }
            break;
        }
        case VECSXP: {
            Rcpp::List lst(value);
            std::vector<graphlab::flexible_type> tmp;
            tmp.push_back(Rcpp::as<graphlab::flexible_type>(lst[0]));
            flex_value = tmp;
            break;
        }
        default: {
            Rcpp::stop("incompatible types encountered");
        }
        }
        return flex_value;
    }
}

template <> SEXP wrap( const graphlab::flexible_type& value) {
    graphlab::flex_type_enum type = value.get_type();
    switch(type) {
    case graphlab::flex_type_enum::STRING: {
        return Rcpp::wrap(value.to<graphlab::flex_string>());
    }
    case graphlab::flex_type_enum::FLOAT: {
        return Rcpp::wrap(value.to<graphlab::flex_float>());
    }
    case graphlab::flex_type_enum::INTEGER: {
        return Rcpp::wrap(value.to<graphlab::flex_int>());
    }
    case graphlab::flex_type_enum::LIST: {
        std::vector<graphlab::flexible_type> f_vec = value.to<graphlab::flex_list>();
        Rcpp::List lst(f_vec.size());
        bool is_vec = true;
        for (size_t i = 0; i < f_vec.size(); i++) {
            graphlab::flex_type_enum type = f_vec[i].get_type();
            switch(type) {
            case graphlab::flex_type_enum::STRING: {
                lst[i] = Rcpp::wrap(f_vec[i].to<graphlab::flex_string>());
                break;
            }
            case graphlab::flex_type_enum::FLOAT: {
                lst[i] = Rcpp::wrap(f_vec[i].to<graphlab::flex_float>());
                break;
            }
            case graphlab::flex_type_enum::INTEGER: {
                lst[i] = Rcpp::wrap(f_vec[i].to<graphlab::flex_int>());
                break;
            }
            case graphlab::flex_type_enum::LIST: {
                is_vec = false;
                lst[i] = Rcpp::wrap(f_vec[i]);
                break;
            }
            default: {
                Rcpp::stop("incompatible types found!");
                break;
            }
            }
        }

        if (is_vec) {
            SEXP as_df_symb = Rf_install("unlist");
            Rcpp::Shield<SEXP> call(Rf_lang2(as_df_symb, lst)) ;
            Rcpp::Shield<SEXP> res(Rcpp_eval(call));
            switch(TYPEOF(lst[0])) {
            case REALSXP: {
                Rcpp::Vector<REALSXP, Rcpp::PreserveStorage> out(res) ;
                return out;
            }
            case INTSXP: {
                Rcpp::Vector<INTSXP, Rcpp::PreserveStorage> out(res) ;
                return out;
            }
            case STRSXP: {
                Rcpp::Vector<STRSXP, Rcpp::PreserveStorage> out(res) ;
                return out;
            }
            }
        } else {
            return lst;
        }
    }
    case graphlab::flex_type_enum::DICT: {
        dict d;
        for(auto imap: value.to<graphlab::flex_dict>())
            d[imap.first] = imap.second;
        return Rcpp::XPtr<dict>(new dict(d), true) ;
    }
    case graphlab::flex_type_enum::UNDEFINED: {
        return Rcpp::wrap(NA_REAL);
    }
    default: {
        Rcpp::stop("incompatible types found!");
        break;
    }
    }
}

}
