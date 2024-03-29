/**
* @file job.h
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
#ifndef CASPER_JOB_SEQUENCER_SEQUENCE_H_
#define CASPER_JOB_SEQUENCER_SEQUENCE_H_

#include "cc/non-movable.h"

#include <inttypes.h> // uint64_t
#include <string>

#include "json/json.h"

#include "casper/job/sequencer/status.h"

#include "casper/job/sequencer/v8/script.h"

namespace casper
{

    namespace job
    {

        namespace sequencer
        {

            class Sequence final : public cc::NonMovable
            {
                
            public: // Enum(s)
                
                enum class Source : uint8_t {
                    Default,
                    Jobification
                };
                
            private: // Data
                
                Source      source_;    //!< One of \link Source \link.
                uint64_t    cid_ ;      //!< CLUSTER ID.
                uint64_t    iid_ ;      //!< INSTANCE ID.
                uint64_t    bjid_;      //!< BEANSTALKD job id ( for logging purposes ).
                std::string rsid_;      //!< REDIS service id.
                uint64_t    rjnr_;      //!< REDIS job number.
                std::string rjid_;      //!< REDIS job key.
                std::string rcid_;      //!< REDIS job channel.
                std::string did_ ;      //!< DB id ( form table js.sequences[id] as string ).
                size_t      count_;     //!< NUMBER of activites related to this sequence.
                Json::Value origin_;    //!< Origin info, if available.
                Json::Value on_error_;  //!< JSON object with 'on_error' config.

            public: // Constructor(s) / Destructor

                Sequence () = delete;
                Sequence (const Source& a_source, const uint64_t& a_cid, const uint64_t& a_iid, const uint64_t& a_bjid,
                          const std::string& a_rsid, const uint64_t& a_rjnr, const std::string& a_rjid, const std::string& a_rcid,
                          const Json::Value& a_origin, const Json::Value& a_on_error);
                Sequence (const Source& a_source, const uint64_t& a_cid, const uint64_t& a_iid, const uint64_t& a_bjid,
                          const std::string& a_rsid, const uint64_t& a_rjnr, const std::string& a_rjid, const std::string& a_rcid, const std::string& a_did,
                          const Json::Value& a_origin, const Json::Value& a_on_error);
                Sequence (const Sequence& a_sequence);
                virtual ~Sequence();
                
            public: // Operator(s) / Overload
                
                Sequence& operator= (const Sequence& a_sequence);
                
            public:
                
                const Source&      source   () const;
                const uint64_t&    cid      () const;
                const uint64_t&    iid      () const;
                const uint64_t&    bjid     () const;
                const std::string& rsid     () const;
                const uint64_t&    rjnr     () const;
                const std::string& rjid     () const;
                const std::string& rcid     () const;
                const std::string& did      () const;
                const size_t&      count    () const;
                const Json::Value& origin   () const;
                const Json::Value& on_error () const;
                
                void               Bind    (const std::string& a_id, const size_t& a_count);

            }; // end of class 'Sequence'
        
            /**
             * @brief Assigment operator overload.
             *
             * @param a_sequence Object to copy.
             *
             * @return Ref to this job.
             */
            inline Sequence& Sequence::operator = (const Sequence& a_sequence)
            {
                source_   = a_sequence.source_;
                cid_      = a_sequence.cid_;
                iid_      = a_sequence.iid_;
                bjid_     = a_sequence.bjid_;
                rsid_     = a_sequence.rsid_;
                rjnr_     = a_sequence.rjnr_;
                rjid_     = a_sequence.rjid_;
                rcid_     = a_sequence.rcid_;
                did_      = a_sequence.did_;
                count_    = a_sequence.count_;
                origin_   = a_sequence.origin_;
                on_error_ = a_sequence.on_error_;
                return *this;
            }
        
            /**
             * @return R/O access to source.
             */
            inline const Sequence::Source& Sequence::source () const
            {
                return source_;
            }
                
            /**
             * @return R/O access to CLUSTER ID.
             */
            inline const uint64_t& Sequence::cid () const
            {
                return cid_;
            }
            
            /**
             * @return R/O access to CLUSTER ID.
             */
            inline const uint64_t& Sequence::iid () const
            {
                return iid_;
            }
            
            /**
             * @return R/O access to BEANSTALKD job id ( for logging purposes ).
             */
            inline const uint64_t& Sequence::bjid () const
            {
                return bjid_;
            }
        
            /**
             * @return R/O access to REDIS service ID.
             */
            inline const std::string& Sequence::rsid () const
            {
                return rsid_;
            }
        
            /**
             * @return R/O access to REDIS job number.
             */
            inline const uint64_t& Sequence::rjnr () const
            {
                return rjnr_;
            }
            
            /**
             * @return R/O access to REDIS job key.
             */
            inline const std::string& Sequence::rjid () const
            {
                return rjid_;
            }
            
            /**
             * @return R/O access to REDIS job channel.
             */
            inline const std::string& Sequence::rcid () const
            {
                return rcid_;
            }
            
            /**
             * @return R/O access to DB id ( form table js.sequences[id] as string ).
             */
            inline const std::string& Sequence::did  () const
            {
                return did_;
            }
        
            /**
             * @return Number of activites related to this sequence.
             */
            inline const size_t& Sequence::count () const
            {
                return count_;
            }
            
            /**
             * @return R/O access to 'Origin' info, optional.
             */
            inline const Json::Value& Sequence::origin() const
            {
                return origin_;
            }
            
            /**
             * @return R/O access to 'on_error' info, optional.
             */
            inline const Json::Value& Sequence::on_error() const
            {
                return on_error_;
            }
        
            /**
             * @brief Set DB id and number of activites related to this sequence..
             *
             * @param a_id DB id as string.
             * @param a_count number of activites related to this sequence.
             */
            inline void Sequence::Bind (const std::string& a_id, const size_t& a_count)
            {
                did_   = a_id;
                count_ = a_count;
            }
        
       } // end of namespace 'sequencer'
   
   } // end of namespace 'job'

} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_SEQUENCE_H_

