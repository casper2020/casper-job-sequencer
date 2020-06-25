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
#pragma once
#ifndef CASPER_JOB_SEQUENCER_V8_SCRIPT_H_
#define CASPER_JOB_SEQUENCER_V8_SCRIPT_H_

#include "cc/v8/script.h"
#include "cc/v8/value.h"

#include "cc/macros.h"

#include "cc/non-movable.h"
#include "cc/non-copyable.h"

namespace casper
{
    
    namespace job
    {
        
        namespace sequencer
        {
            
            namespace v8
            {

                class Script final : public ::cc::v8::Script, public ::cc::NonMovable, public ::cc::NonCopyable
                {

                private: // Static Const Data

                    static const char* const k_evaluate_basic_expression_func_name_;
                    static const char* const k_evaluate_basic_expression_func_;

                    static const char* const k_variable_dump_func_name_;
                    static const char* const k_variable_dump_func_;

                private: // Data
                    
                    ::v8::Local<::v8::Value>                     args_[5];
                    ::cc::v8::Context::LoadedFunction::Callable  callable_;
                    ::v8::Persistent<::v8::Value>                result_;

                public: // Constructor(s) / Destructor
                    
                    Script () = delete;
                    Script (const std::string& a_owner, const std::string& a_name, const std::string& a_uri,
                            const std::string& a_out_path);
                    virtual ~Script ();
                    
                public: // Inherited Method(s) / Function(s) - from ::cc::v8::Script
                    
                    virtual void Load (const Json::Value& a_external_scripts, const Expressions& a_expressions);
                                        
                public: // Method(s) / Function(s)
                    
                    void SetData   (const char* const a_name, const char* const a_data,
                                    ::v8::Persistent<::v8::Object>* o_object = nullptr,
                                    ::v8::Persistent<::v8::Value>* o_value = nullptr,
                                    ::v8::Persistent<::v8::String>* o_key = nullptr) const;
                    
                    void Dump     (const ::v8::Persistent<::v8::Value>& a_object) const;
                    void Evaluate (const ::v8::Persistent<::v8::Value>& a_object, const std::string& a_expr_string,
                                   ::cc::v8::Value& o_value);
                    
                private: // Static Method(s) / Function(s)
                    
                    static void NativeLog                 (const ::v8::FunctionCallbackInfo<::v8::Value>& a_args);
                    static void NativeParseDate           (const ::v8::FunctionCallbackInfo<::v8::Value>& a_args);
                    static void FunctionCallErrorCallback (const ::cc::v8::Context::LoadedFunction::Callable& a_callable, const char* const a_message);
                    
                }; // end of class 'Script'
                
            } // end of namespace 'v8'
            
        } // end of namespace 'sequencer'
        
    } // end of namespace 'job'
    
} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_V8_SCRIPT_H_
