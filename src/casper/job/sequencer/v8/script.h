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

#include "cc/v8/basic/evaluator.h"
#include "cc/v8/script.h"
#include "cc/v8/value.h"

namespace casper
{
    
    namespace job
    {
        
        namespace sequencer
        {
            
            namespace v8
            {

                class Script final : public ::cc::v8::basic::Evaluator
                {

                public: // Constructor(s) / Destructor
                    
                    Script () = delete;
                    Script (const ::ev::Loggable::Data& a_loggable_data,
                            const std::string& a_owner, const std::string& a_name, const std::string& a_uri,
                            const std::string& a_out_path);
                    virtual ~Script ();
                    
                private: // Static Method(s) / Function(s)
                    
                    static void NativeParseDate (const ::v8::FunctionCallbackInfo<::v8::Value>& a_args);
                    static void NativePreserve  (const ::v8::FunctionCallbackInfo<::v8::Value>& a_args);
                    
                }; // end of class 'Script'
                
            } // end of namespace 'v8'
            
        } // end of namespace 'sequencer'
        
    } // end of namespace 'job'
    
} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_V8_SCRIPT_H_
