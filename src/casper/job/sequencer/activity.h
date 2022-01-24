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

#include "cc/easy/json.h"

#include "casper/job/sequencer/exception.h"
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
                                                
                std::string        did_;         //!< DB id ( form table js.activities[id] as string ).
                size_t             index_;       //!< JOB index.
                Json::Value        payload_;     //!< JOB payload.
                size_t             attempt_;     //!< JOB attempt number.
                uint64_t           rjnr_;        //!< REDIS job number.
                std::string        rjid_;        //!< JOB REDIS id.
                std::string        rcnm_;        //!< JOB REDIS channel name.
                std::string        rcid_;        //!< JOB REDIS channel id.
                Status             status_;      //!< JOB status.
                uint32_t           validity_;    //!< JOB validity.
                uint32_t           ttr_;         //!< JOB TTR.
                std::string        abort_expr_;  //!< Optional, abort condition ( V8 expression to evaluate ).
                std::string        abort_msg_;   //!< Optional, abort message.
                
            public: // Constructor(s) / Destructor
                
                Activity () = delete;
                Activity (const Sequence& a_sequence, const std::string& a_did, const size_t& a_index, const size_t& a_attempt);
                Activity (const Activity& a_activity);
                virtual ~Activity();
                
            public: // Method(s) / Function(s)
                
                void      Bind    (const uint64_t a_rjnr, const std::string& a_rjid, const std::string& a_rcnm, const std::string& a_rcid, const bool a_new_attempt);
                Activity& Bind    (const Status& a_status, const uint32_t& a_validity, const uint32_t& a_ttr, const Json::Value& a_payload);
                void      Reset   (const Status& a_status, const Json::Value& a_payload, const uint32_t& a_validity = 0, const uint32_t& a_ttr = 0);
                
                void SetIndex          (const size_t& a_index);
                void SetDID            (const std::string& a_did);
                void SetPayload        (const Json::Value& a_payload);
                void SetStatus         (const Status& a_status);
                void SetValidity       (const uint32_t& a_validity);
                void SetTTR            (const uint32_t& a_ttr);
                void SetAbortCondition (const Json::Value& a_obj);

            public: // RO Method(s) / Function(s)
                
                const Sequence&     sequence   () const;
                const std::string&  did        () const;
                const size_t&       index      () const;
                const Json::Value&  payload    () const;
                const size_t&       attempt    () const;
                const uint64_t&     rjnr       () const;
                const std::string&  rjid       () const;
                const std::string&  rcnm       () const;
                const std::string&  rcid       () const;
                const Status&       status     () const;
                const uint32_t&     validity   () const;
                const uint32_t&     ttr        () const;
                const std::string&  abort_expr () const;
                const std::string&  abort_msg  () const;
                
            public: // Operator(s) / Overload
                
                Activity& operator= (const Activity& a_activity) = delete;
        
            }; // end of class 'Activity'            

            /**
             * @brief Set some of the activity mutable properties related to REDIS.
             *
             * @param a_rjnr
             * @param a_rjid
             * @param a_rcnm
             * @param a_rcid
             * @param a_new_attempt
             */
            inline void Activity::Bind (const uint64_t a_rjnr, const std::string& a_rjid, const std::string& a_rcnm, const std::string& a_rcid, const bool a_new_attempt)
            {
                rjnr_ = a_rjnr;
                rjid_ = a_rjid;
                rcnm_ = a_rcnm;
                rcid_ = a_rcid;
                if ( true == a_new_attempt ) {
                    attempt_ += 1;
                }
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
            inline void Activity::Reset (const Status& a_status, const Json::Value& a_payload, const uint32_t& a_validity, const uint32_t& a_ttr)
            {
                status_      = a_status;
                payload_.clear();
                payload_     = a_payload;
                ttr_         = a_ttr;
                validity_    = a_validity;
                abort_expr_  = "";
                abort_msg_   = "";
            }

            /**
             * @brief Set activity index.
             *
             * @param a_index Index in sequence.
            */
            inline void Activity::SetIndex (const size_t& a_index)
            {
                index_ = a_index;
            }

            /**
             * @brief Set activity database id.
             *
             * @param a_did DB id ( form table js.activities[id] as string ).
            */
            inline void Activity::SetDID (const std::string& a_did)
            {
                did_ = a_did;
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
             * @brief Set activity validity.
             *
             * @param a_validity
             */
            inline void Activity::SetValidity (const uint32_t& a_validity)
            {
                validity_ = a_validity;
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
             * @brief Set abort condition ( V8 expression to evaluate ).
             *
             * @param a_objt Object containing V8 expression to evaluate.
             */
            inline void Activity::SetAbortCondition (const Json::Value& a_obj)
            {
                // ... if no abort object provided ...
                if ( true == a_obj.isNull() ) {
                    // ... nothing to do here ...
                    return;
                }
                // ... obtain abort info ...
                const ::cc::easy::JSON<::cc::Exception> json;
                const auto& expr = json.Get(a_obj, "expr", Json::ValueType::stringValue, &Json::Value::null);
                if ( false == expr.isNull() ) {
                    abort_expr_ = expr.asString();
                    const auto& i18n = json.Get(a_obj, "i18n", Json::ValueType::objectValue, &Json::Value::null);
                    if ( false == i18n.isNull() ) {
                        const auto& aborted = json.Get(i18n, "aborted", Json::ValueType::stringValue, &Json::Value::null);
                        if ( false == aborted.isNull() ) {
                            abort_msg_ = aborted.asString();
                        }
                    }
                }
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
             * @return RO access to activity REDIS job number.
             */
            inline const uint64_t& Activity::rjnr () const
            {
                return rjnr_;
            }
        
            /**
             * @return RO access to activity job REDIS id.
             */
            inline const std::string& Activity::rjid() const
            {
                return rjid_;
            }

            /**
             * @return RO access to ativity job REDIS channel name.
             */
            inline const std::string& Activity::rcnm() const
            {
                return rcnm_;
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
        
            /**
             * @return RO access to activity abort expression.
             */
            inline const std::string& Activity::abort_expr () const
            {
                return abort_expr_;
            }
        
            /**
             * @return RO access to activity abort message.
             */
            inline const std::string& Activity::abort_msg () const
            {
                return abort_msg_;
            }

        } // end of namespace 'sequencer'
    
    } // end of namespace 'job'

} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_ACTIVITY_H_
