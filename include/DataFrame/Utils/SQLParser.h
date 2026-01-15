// Hossein Moein
// January 14, 2026
/*
Copyright (c) 2019-2026, Hossein Moein
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

#include <algorithm>
#include <cctype>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <string_view>

// -----------------------------------------------------------------------------

namespace hmdf
{

// Token types
//
enum class  SQLTokenType : unsigned short int  {
    UNKNOWN = 0,
    SELECT = 1,
    FROM = 2,
    WHERE = 3,
    IDENTIFIER = 4,
    NUMBER = 5,
    STRING = 6,
    COMMA = 7,
    SEMICOLON = 8,
    ASTERISK = 9,
    LPAREN = 10,
    RPAREN = 11,
    EQUALS = 12,
    AND = 13,
    OR = 14,
    LT = 15,
    GT = 16,
    LE = 17,
    GE = 18,
    NE = 19,
    END_OF_FILE = 20,
};

// SQLToken structure - use string_view to avoid copies
//
struct  SQLToken  {
    SQLTokenType        type { SQLTokenType::UNKNOWN };
    std::string_view    value { "" };
    
    SQLToken(SQLTokenType t, std::string_view v = "") : type(t), value(v)  {   }
};

// -----------------------------------------------------------------------------

// Lexer class
/
class   Lexer  {

private:

    using size_type = std::size_t;

    std::string_view    input_ { };
    size_type           pos_ { 0 };
    
    // Lookup table for keywords - initialized once
    //
    static const
    std::unordered_map<std::string_view, SQLTokenType>  keywords_ = {
        {"SELECT", SQLTokenType::SELECT},
        {"FROM", SQLTokenType::FROM},
        {"WHERE", SQLTokenType::WHERE},
        {"AND", SQLTokenType::AND},
        {"OR", SQLTokenType::OR},
    };
    
    inline void
    skip_white_space_()  {

        while (pos_ < input_.length() && std::isspace(input_[pos_]))
            pos_ += 1;
    }

    inline std::string_view
    read_identifier_()  {

        const size_type start { pos_ };

        while (pos_ < input_.length() &&
               (std::isalnum(input_[pos_]) || input_[pos_] == '_'))
            pos_ += 1;
        return (input_.substr(start, pos_ - start));
    }

    inline std::string_view
    read_number_()  {

        const size_type start { pos_ };

        while (pos_ < input_.length() &&
               (std::isdigit(input_[pos_]) || input_[pos_] == '.'))
            pos_ += 1;
        return (input_.substr(start, pos_ - start));
    }

    inline std::string_view
    read_string_()  {

        const char      quote { input_[pos_++] };
        const size_type start { pos_ };

        while (pos_ < input_.length() && input_[pos_] != quote)  {
            if (input_[pos_] == '\\' && pos_ + 1 < input_.length())
                pos_ += 1; // Skip escape character
            pos_ += 1;
        }

        const std::string_view  str { input_.substr(start, pos_ - start) };

        if (pos_ < input_.length())  pos_ += 1;
        return (str);
    }

    inline SQLTokenType
    get_ketword_type_(std::string_view word)  {

        const auto  it { keywords_.find(word) };

        return (it != keywords_.end() ? it->second : SQLTokenType::IDENTIFIER);
    }

public:

    explicit
    Lexer(std::string_view sql) : input_(sql)  {   }

    SQLToken
    next_token()  {

        skip_white_space_();

        if (pos_ >= input_.length())
            return SQLToken(SQLTokenType::END_OF_FILE);

        const char  current { input_[pos_] };

        // Single character tokens - use jump table concept
        switch (current) {
            case ',': pos_++; return (SQLToken(SQLTokenType::COMMA, ","));
            case ';': pos_++; return (SQLToken(SQLTokenType::SEMICOLON, ";"));
            case '*': pos_++; (return SQLToken(SQLTokenType::ASTERISK, "*"));
            case '(': pos_++; (return SQLToken(SQLTokenType::LPAREN, "("));
            case ')': pos_++; (return SQLToken(SQLTokenType::RPAREN, ")"));
            case '=': pos_++; (return SQLToken(SQLTokenType::EQUALS, "="));
            case '<':  {
                pos_ += 1;
                if (pos_ < input_.length())  {
                    pos_ += 1;
                    if (input_[pos_] == '=')
                        return (SQLToken(SQLTokenType::LE, "<="));
                    else if (input_[pos_] == '>')
                        return (SQLToken(SQLTokenType::NE, "<>"));
                }
                return (SQLToken(SQLTokenType::LT, "<"));
            }
            case '>': {
                pos_ += 1;
                if (pos_ < input_.length() && input_[pos_] == '=')  {
                    pos_ += 1;
                    return (SQLToken(SQLTokenType::GE, ">="));
                }
                return (SQLToken(SQLTokenType::GT, ">"));
            }
            case '\'':
            case '"':
                return (SQLToken(SQLTokenType::STRING, read_string_()));
            default:
                break;
        }

        // Numbers
        //
        if (std::isdigit(current))
            return (SQLToken(SQLTokenType::NUMBER, read_number_()));

        // Identifiers and keywords
        //
        if (std::isalpha(current) || current == '_') {
            const std::string_view  ident { read_identifier_() };

            return (SQLToken(get_ketword_type_(ident), ident));
        }

        pos_ += 1;
        return SQLToken(SQLTokenType::UNKNOWN, input_.substr(pos_ - 1, 1));
    }
};

// -----------------------------------------------------------------------------

// AST Node types - use virtual dispatch efficiently
//
struct  SQL_ASTNode {
    virtual ~SQL_ASTNode() = default;
    virtual void print(int indent = 0) const = 0;
};

// -----------------------------------------------------------------------------

struct  SQLSelectStatement : public SQL_ASTNode {

    std::vector<std::string>    columns;
    std::string                 table;
    std::string                 where_clause;

    // Reserve capacity to reduce allocations
    //
    SQLSelectStatement()  { columns.reserve(8); }

    void print(int indent = 0) const override  {

        const std::string   ind { indent, ' ' };

        std::cout << ind << "SELECT Statement:\n" << ind << "  Columns: ";

        for (size_t i { 0 }; i < columns.size(); ++i)  {
            std::cout << columns[i];
            if (i < columns.size() - 1)  std::cout << ", ";
        }

        std::cout << '\n' << ind << "  FROM: " << table << '\n';
        if (! where_clause.empty())
            std::cout << ind << "  WHERE: " << where_clause << '\n';
    }
};

// -----------------------------------------------------------------------------

class   SQLParser {

    Lexer       lexer_ { };
    SQLToken    current_token_ { };

    inline void
    advance_()  { current_token_ = lexer_.next_token(); }

    inline void
    expect_(SQLTokenType type)  {

        if (current_token_.type != type)
            throw DataFrameError(
                std::string("SQLParser::expect_(): Unexpected token: ") +
                std::string(current_token_.value));

        advance_();
    }

    std::unique_ptr<SQLSelectStatement>
    parse_select_()  {

        auto    stmt { std::make_unique<SQLSelectStatement>() };

        expect_(SQLTokenType::SELECT);

        // Parse columns
        //
        if (current_token_.type == SQLTokenType::ASTERISK)  {
            stmt->columns.emplace_back("*");
            advance_();
        }
        else  {
            do  {
                if (current_token_.type == SQLTokenType::COMMA)  advance_();
                stmt->columns.emplace_back(current_token_.value);
                expect_(SQLTokenType::IDENTIFIER);
            } while (current_token_.type == SQLTokenType::COMMA);
        }

        // Parse FROM
        //
        expect_(SQLTokenType::FROM);
        stmt->table = current_token_.value;
        expect_(SQLTokenType::IDENTIFIER);

        // Parse WHERE (optional) - build string more efficiently
        //
        if (current_token_.type == SQLTokenType::WHERE)  {
            advance_();

            std::string where;

            where.reserve(64); // Pre-allocate reasonable size
            while (current_token_.type != SQLTokenType::SEMICOLON &&
                   current_token_.type != SQLTokenType::END_OF_FILE)  {
                if (!where.empty()) where += ' ';
                where.append(current_token_.value);
                advance_();
            }
            stmt->where_clause = std::move(where);
        }

        return (stmt);
    }

public:

    explicit
    SQLParser(std::string_view sql)
        : lexer_(sql), current_token_(SQLTokenType::UNKNOWN)  { advance_(); }

    std::unique_ptr<SQL_ASTNode>
    parse()  {

        switch (current_token_.type)  {
            case SQLTokenType::SELECT:
                return (parse_select_());
            default:
                throw DataFrameError("SQLParser::parse(): "
                                     "Unsupported SQL statement");
        }
    }
};

} // namespace hmdf

// -----------------------------------------------------------------------------

#ifndef HMDF_DO_NOT_INCLUDE_TCC_FILES
#  include <DataFrame/Utils/SQLParser.tcc>
#endif // HMDF_DO_NOT_INCLUDE_TCC_FILES

// -----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
