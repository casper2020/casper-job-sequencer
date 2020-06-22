/**
* @file activity.h
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
#ifndef CASPER_JOB_SEQUENCER_ACTIVITY_H_
#define CASPER_JOB_SEQUENCER_ACTIVITY_H_

#include "cc/non-movable.h"

#include <inttypes.h> // uint64_t
#include <string>

#include "json/json.h"

#include "casper/job/sequencer/status.h"
#include "casper/job/sequencer/sequence.h"

namespace casper
{

    namespace job
    {

        namespace sequencer
        {

            class Activity final : public cc::NonMovable
            {
                
            private: // Const Data
                
                const Sequence    sequence_;    //!< SEQUENCER beanstald job info.
                
            private: // Data
                                                
                std::string        did_;         //!< DB  id ( form table js.sequence[id] as string ).
                size_t             index_;       //!< JOB index.
                Json::Value        payload_;     //!< JOB payload.
                size_t             attempt_;     //!< JOB attempt number.
                std::string        rjid_;        //!< JOB REDIS id.
                std::string        rcid_;        //!< JOB REDIS channel id.
                Status             status_;      //!< JOB status.
                uint32_t           validity_;    //!< JOB validity.
                uint32_t           ttr_;         //!< JOB TTR.
                
            public: // Constructor(s) / Destructor
                
                Activity () = delete;
                Activity (const Sequence& a_sequence, const std::string& a_did, const size_t& a_index, const size_t& a_attempt);
                Activity (const Activity& a_activity);
                virtual ~Activity();
                
            public: // Method(s) / Function(s)
                
                void      Bind    (const std::string& a_rjid, const std::string& a_rcid);
                Activity& Bind    (const Status& a_status, const uint32_t& a_validity, const uint32_t& a_ttr, const Json::Value& a_payload);
                void      Reset   (const Status& a_status, const Json::Value& a_payload, const uint32_t& a_ttr);
                
                void SetPayload (const Json::Value& a_payload);
                void SetStatus  (const Status& a_status);
                void SetTTR     (const uint32_t& a_ttr);

            public: // RO Method(s) / Function(s)
                
                const Sequence&     sequence () const;
                const std::string&  did      () const;
                const size_t&       index    () const;
                const Json::Value&  payload  () const;
                const size_t&       attempt  () const;
                const std::string&  rjid     () const;
                const std::string&  rcid     () const;
                const Status&       status   () const;
                const uint32_t&     validity () const;
                const uint32_t&     ttr      () const;
                
            public: // Operator(s) / Overload
                
                Activity& operator= (const Activity& a_activity) = delete;
        
            }; // end of class 'Activity'            

            /**
             * @brief Set some of the activity mutable properties related to REDIS.
             *
             * @param a_rjid
             * @param a_rcid
             */
            inline void Activity::Bind (const std::string& a_rjid, const std::string& a_rcid)
            {
                rjid_    = a_rjid;
                rcid_    = a_rcid;
            }
        
            /**
             * @brief Set some of the activity mutable properties scheduling.
             *
             * @return Ref to this object instance;
             */
            inline Activity& Activity::Bind (const Status& a_status, const uint32_t& a_validity, const uint32_t& a_ttr, const Json::Value& a_payload)
            {
                status_   = a_status;
                validity_ = a_validity;
                ttr_      = a_ttr;
                payload_.clear();
                payload_ = a_payload;
                return *this;
            }
        
            /**
             * @brief Set some of the activity mutable properties scheduling.
             *
             *  @return Ref to this object instance;
             */
            inline void Activity::Reset (const Status& a_status, const Json::Value& a_payload, const uint32_t& a_ttr)
            {
                status_      = a_status;
                payload_.clear();
                payload_     = a_payload;
                ttr_         = a_ttr;
            }

            /**
             * @brief Set activity payload ptr.
             *
             * @param a_payload
             */
            inline void Activity::SetPayload (const Json::Value& a_payload)
            {
                payload_.clear();
                payload_ = a_payload;
            }
        
            /**
             * @brief Set activity status.
             *
             * @param a_status
             */
            inline void Activity::SetStatus (const Status& a_status)
            {
                status_ = a_status;
            }
        
            /**
             * @brief Set activity TTR.
             *
             * @param a_ttr
             */
            inline void Activity::SetTTR (const uint32_t& a_ttr)
            {
                ttr_ = a_ttr;
            }
        
            /**
             * @return RO access to sequence info.
             */
            inline const Sequence& Activity::sequence () const
            {
                return sequence_;
            }

            /**
             * @return RO access to activity DB id ( .sequence[id] as string ).
             */
            inline const std::string& Activity::did() const
            {
                return did_;
            }
        
            /**
             * @return RO access to activity job index.
             */
            inline const size_t& Activity::index() const
            {
                return index_;
            }
        
            /**
             * @return RO access to activity job payload.
             */
            inline const Json::Value& Activity::payload() const
            {
                return payload_;
            }
        
            /**
             * @return RO access to activity job attempt number.
             */
            inline const size_t& Activity::attempt() const
            {
                return attempt_;
            }
        
            /**
             * @return RO access to activity job REDIS id.
             */
            inline const std::string& Activity::rjid() const
            {
                return rjid_;
            }
        
            /**
             * @return RO access to ativity job REDIS channel id.
             */
            inline const std::string& Activity::rcid() const
            {
                return rcid_;
            }
        
            /**
             * @return RO access to activity job status.
             */
            inline const Status& Activity::status() const
            {
                return status_;
            }
            
            /**
             * @return RO access to activity job TTR.
             */
            inline const uint32_t& Activity::ttr() const
            {
                return ttr_;
            }

        } // end of namespace 'sequencer'
    
    } // end of namespace 'job'

} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_ACTIVITY_H_
