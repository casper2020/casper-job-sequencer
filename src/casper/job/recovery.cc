/**
* @file recovery.cc
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

#include "casper/job/recovery.h"

#include "cc/macros.h"
#include "cc/exception.h"

#ifdef __APPLE__
#pragma mark - casper::job::Recovery
#endif

const char* const casper::job::Recovery::s_tube_ = "sequencer-recovery";

/**
 * @brief Default constructor.
 *
 * param a_loggable_data
 * param a_config
 */
casper::job::Recovery::Recovery (const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config)
    : casper::job::Sequencer(s_tube_, a_loggable_data, a_config)
{
    /* empty */
}

/**
 * @brief Destructor
 */
casper::job::Recovery::~Recovery ()
{
    /* empty */
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Process a job to this tube.
 *
 * @param a_id      Job ID.
 * @param a_payload Job payload.
 *
 * @param o_response JSON object.
 */
void casper::job::Recovery::Run (const int64_t& a_id, const Json::Value& a_payload,
                                 cc::job::easy::Job::Response& o_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id());
    
    //   {
    //      "id": "1",
    //      "ttr": 360,
    //      "valitity": 500,
    //      "tube": "sequencer-recovery",
    //      "sequence": 1
    //  }

    // ... start as a 'bad request' ...
    o_response.code_ = 400;
    
    // ... validate job payload ...
    CC_WARNING_TODO("CJS: validate job payload");
    
    CC_WARNING_TODO("CJS: Recovery JOB Run");
    throw cc::Exception("TODO: Recovery JOB Run!");
}
