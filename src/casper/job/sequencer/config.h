/**
* @file coinfig.h
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
#ifndef CASPER_JOB_SEQUENCER_CONFIG_H_
#define CASPER_JOB_SEQUENCER_CONFIG_H_

#include "cc/non-movable.h"
#include "cc/non-copyable.h"

#include "json/json.h"

namespace casper
{

        namespace job
        {

            namespace sequencer
            {
            
                class Config final : public cc::NonMovable, public cc::NonCopyable
                {
                    
                public: // Const Data
                    
                    const Json::Value validity_;
                    const Json::Value ttr_;
                    
                public: // Constructor(s) / Destructor
                    
                    Config () = delete;
                    
                    /**
                     * @brief Default constructor,
                     *
                     * @param a_config JSON value to load.
                     */
                    Config (const Json::Value& a_config)
                        : validity_(a_config.get("validity", static_cast<Json::UInt64>(3600)).asUInt()),
                          ttr_(a_config.get("ttr", static_cast<Json::UInt64>(300)).asUInt())
                    {
                        /* empty */
                    }
                    
                    /**
                     @brief Destructor.
                     */
                    virtual ~Config ()
                    {
                        /* empty */
                    }
                                    
                }; // end of class 'Config'
            
            } // end of namespace 'sequencer'

        } // end of namespace 'job'

} // end of namespace 'casper'
        
#endif // CASPER_JOB_SEQUENCER_CONFIG_H_
