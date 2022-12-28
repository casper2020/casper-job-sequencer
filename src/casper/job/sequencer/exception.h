/**
* @file exception.h
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
#ifndef CASPER_JOB_SEQUENCER_EXCEPTION_H_
#define CASPER_JOB_SEQUENCER_EXCEPTION_H_

#include "cc/exception.h"
#include "cc/v8/exception.h"

namespace casper
{

    namespace job
    {

        namespace sequencer
        {
        
            //
            // TRACKING
            //
            typedef struct {
                const uint64_t    bjid_;
                const std::string action_;
                const std::string file_;
                const std::string function_;
                const int         line_;
            } Tracking;

            #define SEQUENCER_TRACK_CALL(a_bjid, a_action) \
                { a_bjid, a_action, __FILE__, __PRETTY_FUNCTION__, __LINE__ }

            //
            // EXCEPTION
            //
            class Exception : public ::cc::Exception
            {
                
            public: // Const Data
                
                const Tracking tracking_;   //!< Tracking info.
                const uint16_t code_;       //!< HTTP Status Code
                                
            public: // Constructor(s) / Destructor
                
                Exception (const Tracking& a_tracking, const uint16_t& a_code, const char* const a_why)
                    : ::cc::Exception("%s", a_why),
                      tracking_(a_tracking), code_(a_code)
                {
                    /* empty */
                }
                
                virtual ~Exception ()
                {
                    /* empty */
                }
                
            };
            
            class JumpErrorAlreadySet final : public Exception
            {
                
            public: // Constructor(s) / Destructor
                
                JumpErrorAlreadySet (const Tracking& a_tracking, const uint16_t& a_code, const char* const a_why)
                    : Exception(a_tracking, a_code, a_why)
                {
                    /* empty */
                }
                
                virtual ~JumpErrorAlreadySet ()
                {
                    /* empty */
                }
                
            };
            
            class BadRequestException final : public Exception
            {
            public: // Constructor(s) / Destructor
                
                BadRequestException (const Tracking& a_tracking, const char* const a_why)
                    : Exception(a_tracking, /* a_code */ 400, a_why)
                {
                    /* empty */
                }
                
                virtual ~BadRequestException ()
                {
                    /* empty */
                }
            };

            typedef BadRequestException JSONValidationException;
            
            class V8ExpressionEvaluationException final : public Exception
            {
                
            public: // Constructor(s) / Destructor
                
                V8ExpressionEvaluationException (const Tracking& a_tracking, const char* const a_why) = delete;
                
                V8ExpressionEvaluationException (const Tracking& a_tracking, const ::cc::v8::Exception& a_v8e)
                    : Exception(a_tracking, /* a_code */ 400, /* a_why */ ( "\n" + std::string(a_v8e.what()) + "\n" ).c_str())
                {
                    /* empty */
                }
                
                virtual ~V8ExpressionEvaluationException ()
                {
                    /* empty */
                }
                
            };

        } // end of namespace 'sequencer'

    } // end of namespace 'job'

} // end of namespace 'casper'
#endif // CASPER_JOB_SEQUENCER_EXCEPTION_H_
