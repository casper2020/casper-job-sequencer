/**
* @file main.cc
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

#include <stdio.h>

#include "version.h"

#include "cc/job/easy/handler.h"

#include "casper/job/live.h"
#include "casper/job/recovery.h"

/**
 * @brief Main.
 *
 * param argc
 * param argv
 *
 * return
 */
int main(int argc, char** argv)
{
    const char* short_info = strrchr(CASPER_JOB_SEQUENCER_INFO, '-');
    if ( nullptr == short_info ) {
        short_info = CASPER_JOB_SEQUENCER_INFO;
    } else {
        short_info++;
    }
    
    //
    // LOG FILTERING:
    //
    // tail -f /usr/local/var/log/casper-job-sequencer/sequencer-live.1.log | grep -E ', .*(SEQUENCE|ACTIVITY).*:'
    //
    
    cc::job::easy::Handler::GetInstance().Start(
        /* a_arguments */
        {
            /* abbr_           */ CASPER_JOB_SEQUENCER_ABBR,
            /* name_           */ CASPER_JOB_SEQUENCER_NAME,
            /* version_        */ CASPER_JOB_SEQUENCER_VERSION,
            /* rel_date_       */ CASPER_JOB_SEQUENCER_VERSION,
            /* info_           */ short_info, // short version of CASPER_JOB_SEQUENCER_INFO
            /* banner_         */ CASPER_JOB_SEQUENCER_BANNER,
            /* argc_           */ argc,
            /* argv_           */ const_cast<const char** const >(argv),
        },
        /* a_factories */
        {
            {
                casper::job::Live::s_tube_, [] (const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config) -> cc::job::easy::Job* {
                    return new casper::job::Live(a_loggable_data, a_config);
                }
            },
            {
                casper::job::Recovery::s_tube_, [] (const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config) -> cc::job::easy::Job* {
                    return new casper::job::Recovery(a_loggable_data, a_config);
                }
            }
        },
        /* a_polling_timeout */ 20.0 /* milliseconds */
    );
}
