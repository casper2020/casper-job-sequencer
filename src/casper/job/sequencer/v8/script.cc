/**
 * @file script.h
 *
 * Copyright (c) 2011-2020 Cloudware S.A. All rights reserved.
 *
 * This file is part of casper-job-sequencer.
 *
 * casper-job-sequencer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * casper-job-sequencer  is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with casper-job-sequencer.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "casper/job/sequencer/v8/script.h"

#include "unicode/ustring.h"
#include "unicode/unistr.h"
#include "unicode/decimfmt.h" // U_ICU_NAMESPACE::Locale ...
#include "unicode/datefmt.h"  // U_ICU_NAMESPACE::DateFormat
#include "unicode/smpdtfmt.h" // U_ICU_NAMESPACE::SimpleDateFormat
#include "unicode/dtfmtsym.h" // U_ICU_NAMESPACE::DateFormatSymbols
#include "unicode/schriter.h" // U_ICU_NAMESPACE::StringCharacterIterator

/**
 * @brief Default constructor.
 *
 * @param a_loggable_data TO BE COPIED
 * @param a_owner         Script owner.
 * @param a_name          Script name
 * @param a_uri           Unused.
 * @param a_out_path      Writable directory.
 */
casper::job::sequencer::v8::Script::Script (const ::ev::Loggable::Data& a_loggable_data,
                                            const std::string& a_owner, const std::string& a_name, const std::string& a_uri,
                                            const std::string& a_out_path)
    : ::cc::v8::basic::Evaluator(a_loggable_data, a_owner, a_name, a_uri, a_out_path,
                                 /* a_functions */
                                 {
                                     { "NativeLog"      , casper::job::sequencer::v8::Script::NativeLog       },
                                     { "NativeParseDate", casper::job::sequencer::v8::Script::NativeParseDate },
                                     { "CJSPRSRV"       , casper::job::sequencer::v8::Script::NativePreserve  }
                                 }
    )
{
    callable_.on_error_ = std::bind(&casper::job::sequencer::v8::Script::FunctionCallErrorCallback, std::placeholders::_1,  std::placeholders::_2);
}

/**
 * @brief Destructor.
 */
casper::job::sequencer::v8::Script::~Script ()
{
    /* empty */
}

// MARK: -

/**
 * @brief The callback that is invoked by v8 whenever the JavaScript 'NativeParseDate' function is called.
 *
 * @param a_args V8 arguments ( including function args ).
 */
void casper::job::sequencer::v8::Script::NativeParseDate (const ::v8::FunctionCallbackInfo<::v8::Value>& a_args)
{
    const ::v8::HandleScope handle_scope(a_args.GetIsolate());
    
    a_args.GetReturnValue().SetUndefined();

    if ( 3 != a_args.Length() ||
        ( true == a_args[0].IsEmpty() || false == a_args[0]->IsString() )
        ||
        ( true == a_args[1].IsEmpty() || false == a_args[1]->IsString() )
        ||
        ( true == a_args[2].IsEmpty() || false == a_args[2]->IsString() )
    ) {
        return;
    }
    
    const ::v8::String::Utf8Value& value  = ::v8::String::Utf8Value(a_args.GetIsolate(), a_args[0]);
    const ::v8::String::Utf8Value& fmt    = ::v8::String::Utf8Value(a_args.GetIsolate(), a_args[1]);
    const ::v8::String::Utf8Value& locale = ::v8::String::Utf8Value(a_args.GetIsolate(), a_args[2]);
    
    const char* const value_c_str = *value;
    const char* const fmt_c_str   = *fmt;
    const char* const locale_c_str = *locale;
    
    const U_ICU_NAMESPACE::Locale icu_locale = U_ICU_NAMESPACE::Locale::createFromName(locale_c_str);
    if ( false == icu_locale.isBogus() && 0 !=  icu_locale.getCountry()[0] ) {
        UErrorCode error_code = UErrorCode::U_ZERO_ERROR;
        U_ICU_NAMESPACE::SimpleDateFormat* date_format = new U_ICU_NAMESPACE::SimpleDateFormat(U_ICU_NAMESPACE::UnicodeString(fmt_c_str), icu_locale, error_code);
        if ( NULL != date_format  ){
            if ( UErrorCode:: U_ZERO_ERROR == error_code || UErrorCode::U_USING_DEFAULT_WARNING == error_code || UErrorCode::U_USING_FALLBACK_WARNING == error_code ) {
                const UDate parsed_date = date_format->parse(U_ICU_NAMESPACE::UnicodeString(value_c_str), error_code);
                if ( UErrorCode::U_ZERO_ERROR == error_code || UErrorCode::U_USING_DEFAULT_WARNING == error_code || UErrorCode::U_USING_FALLBACK_WARNING == error_code ) {
                    if ( -3600000 != parsed_date ) {
                        a_args.GetReturnValue().Set(parsed_date);
                    }
                }
            }
            delete date_format;
        }
    }
}

/**
 * @brief The callback that is invoked by v8 whenever the JavaScript 'Preserve' function is called.
 *
 * @param a_args V8 arguments ( including function args ).
 */
void casper::job::sequencer::v8::Script::NativePreserve (const ::v8::FunctionCallbackInfo<::v8::Value>& a_args)
{
    const ::v8::HandleScope handle_scope(a_args.GetIsolate());
    if ( 1 != a_args.Length() || true == a_args[0].IsEmpty() ) {
        a_args.GetReturnValue().SetUndefined();
    } else {
        a_args.GetReturnValue().Set(a_args[0]);
    }
}
