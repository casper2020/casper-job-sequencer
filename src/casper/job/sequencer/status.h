/**
* @file status.h
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
#ifndef CASPER_JOB_SEQUENCER_STATUS_H_
#define CASPER_JOB_SEQUENCER_STATUS_H_

namespace casper
{

        namespace job
        {

            namespace sequencer
            {
            
                /*
                 * Status enum.
                 */
                enum class Status : uint8_t {
                    NotSet = 0x00,
                    Pending,
                    Scheduled,
                    InProgress,
                    Done,
                    Cancelled,
                    Failed
                };
                
                /**
                 * @brief Append a \link Synchronization::Status \link value ( string representation ) to a stream.
                 *
                 * @param a_operation One of \link Synchronization::Status \link.
                 */
                inline std::ostream& operator << (std::ostream& o_stream, const sequencer::Status& a_status)
                {
                    switch (a_status) {
                        case sequencer::Status::Pending     : o_stream << "Pending"     ; break;
                        case sequencer::Status::Scheduled   : o_stream << "Scheduled"   ; break;
                        case sequencer::Status::InProgress  : o_stream << "InProgress"  ; break;
                        case sequencer::Status::Done        : o_stream << "Done"        ; break;
                        case sequencer::Status::Cancelled   : o_stream << "Cancelled"   ; break;
                        case sequencer::Status::Failed      : o_stream << "Failed"      ; break;
                        default                             : o_stream.setstate(std::ios_base::failbit); break;
                    }
                    return o_stream;
                }
            
            } // end of namespace 'sequencer'

        } // end of namespace 'job'

} // end of namespace 'casper'
        
#endif // CASPER_JOB_SEQUENCER_STATUS_H_

