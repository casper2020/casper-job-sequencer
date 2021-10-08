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

#include "cc/easy/job/job.h"

#include "json/json.h"

#include "ev/postgresql/request.h"
#include "ev/postgresql/value.h"

#include "casper/job/sequencer/config.h"
#include "casper/job/sequencer/exception.h"
#include "casper/job/sequencer/activity.h"

#include "cc/v8/exception.h"

#include "casper/job/sequencer/v8/script.h"

namespace casper
{

    namespace job
    {
    
        class Sequencer : public cc::easy::job::Job
        {

#define SEQUENCER_LOG_KEY_JOB       "JOB"
#define SEQUENCER_LOG_KEY_SEQUENCE  "SEQUENCE"
#define SEQUENCER_LOG_KEY_ACTIVITY  "ACTIVITY"

#define SEQUENCER_LOG_SEQUENCE(a_level, a_sequence, a_step, a_format, ...) \
    CC_JOB_LOG(a_level, a_sequence.bjid(), \
                CC_JOB_LOG_COLOR(LIGHT_BLUE) "%-8.8s" CC_LOGS_LOGGER_RESET_ATTRS ": %-7.7s, " a_format, \
                SEQUENCER_LOG_KEY_SEQUENCE , a_step, __VA_ARGS__ \
    );

#define SEQUENCER_LOG_ACTIVITY(a_level, a_activity, a_step, a_format, ...) \
    CC_JOB_LOG(a_level, a_activity.sequence().bjid(), \
               CC_JOB_LOG_COLOR(WHITE) "%-8.8s" CC_LOGS_LOGGER_RESET_ATTRS ": %-7.7s, { " SIZET_FMT "/" SIZET_FMT " } " a_format, \
               SEQUENCER_LOG_KEY_ACTIVITY, a_step, ( a_activity.index() + 1 ), a_activity.sequence().count(), __VA_ARGS__ \
    );

#define SEQUENCER_LOG_JOB(a_level, a_bjid, a_step, a_format, ...) \
    CC_JOB_LOG(a_level, a_bjid, \
                CC_JOB_LOG_COLOR(MAGENTA) "%-8.8s" CC_LOGS_LOGGER_RESET_ATTRS ": %-7.7s, " a_format, \
                SEQUENCER_LOG_KEY_JOB, a_step, __VA_ARGS__ \
    );

#define SEQUENCER_LOG_CRITICAL_EXCEPTION(a_format, ...) \
    CC_JOB_LOG(CC_JOB_LOG_LEVEL_CRT, uint64_t(0), \
                CC_JOB_LOG_COLOR(LIGHT_RED) "%-8.8s" CC_LOGS_LOGGER_RESET_ATTRS ": %-7.7s, " a_format, \
                SEQUENCER_LOG_KEY_SEQUENCE, CC_JOB_LOG_STEP_ERROR, __VA_ARGS__ \
    );

        public: // Static Const Data
            
            static const char* const s_schema_;
            static const char* const s_table_;

        private: // Static Const Data
            
            static const std::map<std::string, sequencer::Status> s_irj_teminal_status_map_;

        private: // Data

            sequencer::Config                           sequence_config_;
            sequencer::Config                           activity_config_;
            
            std::map<std::string, sequencer::Activity*> running_activities_; //!< RCID ( REDIS Channel ID ) -> Activity
            casper::job::sequencer::v8::Script*         script_;

        public: // Constructor(s) / Destructor
            
            Sequencer () = delete;
            Sequencer (const char* const a_tube, const ev::Loggable::Data& a_loggable_data, const cc::easy::job::Job::Config& a_config);
            virtual ~Sequencer ();

        protected: // Inherited Virtual Method(s) / Function(s) - from cc::job::easy::Runnable
            
            virtual void Setup     ();
            virtual void Dismantle ();

        protected: // Method(s) / Function(s)
            
            //
            // SEQUENCER
            //
            sequencer::Activity                              RegisterSequence              (sequencer::Sequence& a_sequence, const Json::Value& a_payload);
            void                                             CancelSequence                (const sequencer::Activity& a_activity, const Json::Value& a_response);
            void                                             FinalizeSequence              (const sequencer::Activity& a_activity, const Json::Value& a_response,
                                                                                            double& o_rtt);
            
            //
            // ACTIVITY
            //
            uint16_t                                         LaunchActivity                (const sequencer::Tracking& a_tracking, sequencer::Activity& a_activity, const bool a_at_run);
            void                                             ActivityMessageRelay          (const sequencer::Tracking& a_tracking, const sequencer::Activity& a_activity, const Json::Value& a_message);
            void                                             ActivityReturned              (const sequencer::Tracking& a_tracking, const sequencer::Activity& a_activity, const Json::Value* a_response);
            
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
            
            void                                             CancelActivity                (const sequencer::Activity& a_activity, const Json::Value& a_response);

            void                                             TrackActivity                 (const sequencer::Activity& a_activity);
            void                                             TrackActivity                 (sequencer::Activity* a_activity);
            
            void                                             UntrackActivity               (const sequencer::Activity& a_activity);
            
            void                                             OnActivityTimeout             (const std::string& a_rcid);

            //
            // JOB
            //
            void                                             FinalizeJob                     (const sequencer::Sequence& a_sequence,
                                                                                              const Json::Value& a_response);
            void                                             OnJobsSignalReceived            (const uint64_t& a_id, const std::string& a_status, const Json::Value& a_message);

            //
            // POSTGRESQL
            //
            void                                            ExecuteQueryAndWait            (const sequencer::Tracking& a_tracking,
                                                                                            const std::string& a_query, const ExecStatusType& a_expected,
                                                                                            const std::function<void(const Json::Value& a_value)> a_success_callback = nullptr,
                                                                                            const std::function<void(const ev::Exception& a_exception)> a_failure_callback = nullptr);
                                                                                            
            const ::ev::postgresql::Value*                  EnsurePostgreSQLValue          (const ::ev::Object* a_object, const ExecStatusType& a_expected);
                        
            //
            // SERIALIZATION HELPERS
            //
            const Json::Value& MSG2JSON           (const std::string& a_value, Json::Value& o_value);
            
            void               PatchActivity      (const sequencer::Tracking& a_tracking, sequencer::Activity& a_activity);
            
            void               PatchObject        (Json::Value& a_value, const std::function<Json::Value(const std::string& a_expression)>& a_callback);
            
            //
            // DEBUG HELPER(S)
            //
#if defined(__APPLE__) && !defined(NDEBUG) && ( defined(DEBUG) || defined(_DEBUG) || defined(ENABLE_DEBUG) )
            void               Sleep (const sequencer::Config& a_config, const sequencer::Activity& a_activity, const char* const a_msg);
#endif
            
        protected: // Inline Method(s) // Function(s)
            
            void        LogStats () const;
            std::string MakeID   (const char* const a_name, const std::string a_rcid);

        }; // end of class 'Sequencer'
    
        /**
         * @brief Log some statistics.
         */
        inline void Sequencer::LogStats () const
        {
            CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
            owner_log_callback_(tube_.c_str(),
                                "STATS",
                                std::to_string(running_activities_.size()) + " " + ( 1 == running_activities_.size()  ? "activity is"  : "activities are" ) + " running"
            );
        }
        
        /**
         * @brief Build an ID with a random part.
         *
         * @param a_name Tag.
         * @param a_rcid Activity REDIS channel id.
         *
         * @return Ramdom ID.
         */
        inline std::string Sequencer::MakeID (const char* const a_name, const std::string a_rcid)
        {
            static const char alphanum[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            std::stringstream ss;
            for ( int idx = 0; idx < 32; ++idx ) {
                ss << alphanum[random() % 62];
            }
            return std::string(a_name) + "-" + a_rcid + "-" + ss.str();
        }
              
    } // end of namespace 'job'

} // end of namespace 'casper'

#endif // CASPER_JOB_SEQUENCER_H_
