/**
* @file sequencer.h
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
#ifndef CASPER_JOB_SEQUENCER_H_
#define CASPER_JOB_SEQUENCER_H_

#include "cc/job/easy/job.h"

#include "json/json.h"

#include "ev/postgresql/request.h"
#include "ev/postgresql/value.h"

#include "casper/job/sequencer/activity.h"

#include "cc/v8/exception.h"

#include "casper/job/sequencer/v8/script.h"

namespace casper
{

    namespace job
    {
    
        class Sequencer final : public cc::job::easy::Job
        {

        public: // Static Const Data
            
            static const char* const s_tube_;
            static const char* const s_schema_;
            static const char* const s_table_;
            
        private: // Data Type(s)
            
            typedef struct {
                const int64_t     bjid_;
                const std::string action_;
                const std::string file_;
                const std::string function_;
                const int         line_;
            } Tracking;
            
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

        private: // Static Const Data
            
            static const std::map<std::string, sequencer::Status> s_irj_teminal_status_map_;

        private: // Const Data
            
            const cc::debug::Threading::ThreadID thread_id_;

        private: // Data
            
            std::map<std::string, sequencer::Activity*> running_activities_; //!< RCID ( REDIS Channel ID ) -> Activity
            casper::job::sequencer::v8::Script*         script_;

        public: // Constructor(s) / Destructor
            
            Sequencer () = delete;
            Sequencer (const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config);
            virtual ~Sequencer ();

        public: // Inherited Virtual Method(s) / Function(s) - from cc::job::easy::Runnable
            
            virtual void Setup ();
            virtual void Run   (const int64_t& a_id, const Json::Value& a_payload,
                                cc::job::easy::Job::Response& o_response);

        private: // Method(s) / Function(s)
            
            //
            // SEQUENCER
            //
            sequencer::Activity                              RegisterSequence              (sequencer::Sequence& a_sequence, const Json::Value& a_payload);
            void                                             FinalizeSequence              (const sequencer::Activity& a_activity, const Json::Value& a_response);
            
            //
            // ACTIVITY
            //
            uint16_t                                         LaunchActivity                (const Tracking& a_tracking, sequencer::Activity& a_activity, const bool a_at_run);
            void                                             ActivityMessageRelay          (const Tracking& a_tracking, const sequencer::Activity& a_activity, const Json::Value& a_message);
            void                                             ActivityReturned              (const Tracking& a_tracking, const sequencer::Activity& a_activity, const Json::Value* a_response);
            
            void                                             RegisterActivity              (const sequencer::Activity& a_activity);
            
            // REDIS
            void                                             SubscribeActivity             (const sequencer::Activity& a_activity);
            EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK OnActivityMessageReceived     (const std::string& a_id, const std::string& a_message);
            void                                             UnsubscribeActivity           (const sequencer::Activity& a_activity);
            
            // BEANSTALKD
            void                                             PushActivity                  (const sequencer::Activity& a_activity);

            // REDIS
            void                                             FinalizeActivity              (const sequencer::Activity& a_activity, const Json::Value* a_response,
                                                                                            sequencer::Activity& a_next);
            void                                             TrackActivity                 (const sequencer::Activity& a_activity);
            void                                             TrackActivity                 (sequencer::Activity* a_activity);
            
            void                                             UntrackActivity               (const sequencer::Activity& a_activity);

            //
            // JOB
            //
            void                                            FinalizeJob                    (const sequencer::Sequence& a_sequence,
                                                                                            const Json::Value& a_response);
            
            //
            // POSTGRESQL
            //
            void                                            ExecuteQueryAndWait            (const Tracking& a_tracking,
                                                                                            const std::string& a_query, const ExecStatusType& a_expected,
                                                                                            const std::function<void(const Json::Value& a_value)> a_success_callback = nullptr,
                                                                                            const std::function<void(const ev::Exception& a_exception)> a_failure_callback = nullptr);
                                                                                            
            const ::ev::postgresql::Value*                  EnsurePostgreSQLValue          (const ::ev::Object* a_object, const ExecStatusType& a_expected);
                        
            //
            // OTHER HELPERS
            //
            void                                            TrapUnhandledExceptions        (const char* const a_type, const std::function<void()>& a_callback,
                                                                                            const Tracking& a_tracking);
            //
            // SERIALIZATION HELPERS
            //
            const Json::Value& AsJSON (const std::string& a_value, Json::Value& o_value);
            
            void               PatchActivity      (const Tracking& a_tracking, sequencer::Activity& a_activity);
            
            void               PatchObject        (Json::Value& a_value, const std::function<Json::Value(const std::string& a_expression)>& a_callback);

        }; // end of class 'Sequencer'
              
    } // end of namespace 'job'

} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_H_
