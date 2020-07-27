/**
* @file live.h
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
#ifndef CASPER_JOB_LIVE_H_
#define CASPER_JOB_LIVE_H_

#include "casper/job/sequencer.h"

namespace casper
{

    namespace job
    {
    
        class Live final : public Sequencer
        {

        public: // Static Const Data
            
            static const char* const s_tube_;

        public: // Constructor(s) / Destructor
            
            Live () = delete;
            Live (const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config);
            virtual ~Live ();
            
        public: // Inherited Virtual Method(s) / Function(s) - from ::Sequencer
            
            virtual void Run   (const int64_t& a_id, const Json::Value& a_payload,
                                cc::job::easy::Job::Response& o_response);

        }; // end of class 'Live'
              
    } // end of namespace 'job'

} // end of namespace 'casper'

#endif // CASPER_JOB_LIVE_H_
